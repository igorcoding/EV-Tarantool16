#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#include "EVAPI.h"
#define XSEV_CON_HOOKS 1
#include "xsevcnn.h"

#define MYDEBUG

#include "xstnt16.h"

#if __GNUC__ >= 3
# define INLINE static inline
#else
# define INLINE static
#endif

#define TNT_CROAK(msg) STMT_START {     \
	croak("[Tarantool Error] %s", msg); \
} STMT_END

#define PE_CROAK(msg) STMT_START {    		\
	croak("[Programming Error] %s", msg); 	\
} STMT_END

typedef struct {
	xs_ev_cnn_struct;

	void (*on_disconnect_before)(void *, int);
	void (*on_disconnect_after)(void *, int);
	void (*on_connect_before)(void *, struct sockaddr *);
	void (*on_connect_after)(void *, struct sockaddr *);

	c_cb_conn_t default_on_connected_cb;
	struct sockaddr peer_info;

	uint32_t pending;
	uint32_t seq;
	U32      use_hash;
	HV      *reqs;
	HV      *spaces;
	SV      *username;
	SV      *password;
} TntCnn;

static const uint32_t _SPACE_SPACEID = 280;
static const uint32_t _INDEX_SPACEID = 288;

void tnt_on_connected_cb(ev_cnn *cnn, struct sockaddr *peer) {
	if (likely(peer != NULL)) {
		TntCnn *self = (TntCnn *) cnn;
		self->peer_info = *peer;
	}
}

static inline void call_connected(TntCnn *self) {
	self->default_on_connected_cb(&self->cnn, &self->peer_info);
}

static inline void force_disconnect(TntCnn *self, const char *reason) {
	// croak("Error happened, but no further action provided. Panda is sad.");
	on_connect_reset(&self->cnn, 0, reason);
}

static void on_request_timer(EV_P_ ev_timer *t, int flags) {
	TntCtx * ctx = (TntCtx *) t;
	TntCnn * self = (TntCnn *) ctx->self;
	cwarn("timer called on %p: %s", ctx, ctx->call);
	ENTER;SAVETMPS;
	dSP;

	// ev_timer_stop(self->cnn.loop, &ctx->t);
	// do_disable_rw_timer(&self->cnn);
	SvREFCNT_dec(ctx->wbuf);
	if (ctx->f.size && !ctx->f.nofree) {
		safefree(ctx->f.f);
	}

	if (ctx->cb) {
		SPAGAIN;
		ENTER; SAVETMPS;

		PUSHMARK(SP);
		EXTEND(SP, 2);
		PUSHs( &PL_sv_undef );
		PUSHs( sv_2mortal(newSVpvf("Request timed out")) );
		PUTBACK;

		(void) call_sv( ctx->cb, G_DISCARD | G_VOID );

		//SPAGAIN;PUTBACK;

		SvREFCNT_dec(ctx->cb);

		FREETMPS; LEAVE;
	}

	(void) hv_delete( self->reqs, (char *) &ctx->id, sizeof(ctx->id),0);

	--self->pending;

	FREETMPS;LEAVE;
}

#define TIMEOUT_TIMER(self, iid, timeout) STMT_START {\
	if (timeout > 0) {\
		ev_timer_init(&ctx->t, on_request_timer, timeout, 0.);\
		ev_timer_start(self->cnn.loop, &ctx->t);\
	}\
} STMT_END

#define INIT_TIMEOUT_TIMER(self, iid, opts) STMT_START {\
	double timeout;\
	SV **key;\
	\
	if ( opts && (key = hv_fetchs( opts, "timeout", 0 ))) {\
		timeout = SvNV( *key );\
		/*cwarn("timeout set: %f",timeout);*/\
	} else {\
		timeout = self->cnn.rw_timeout;\
	}\
	TIMEOUT_TIMER(self, iid, timeout);\
} STMT_END

#define EXEC_REQUEST(self, ctxsv, ctx, iid, pkt, _cb) STMT_START {\
	if ((ctx->wbuf = pkt)) {\
		SvREFCNT_inc(ctx->cb = (_cb));\
		(void) hv_store( self->reqs, (char*)&iid, sizeof(iid), SvREFCNT_inc(ctxsv), 0 );\
		++self->pending;\
		do_write(&self->cnn,SvPVX(ctx->wbuf), SvCUR(ctx->wbuf));\
	}\
} STMT_END

INLINE void _execute_select(TntCnn *self, uint32_t space_id) {
	dSVX(ctxsv, ctx, TntCtx);
	sv_2mortal(ctxsv);
	ctx->self = self;
	ctx->call = "select";
	ctx->use_hash = self->use_hash;
	uint32_t iid = ++self->seq;
	ctx->id = iid;
	SV *pkt = pkt_select(ctx, iid, self->spaces, sv_2mortal(newSVuv(space_id)), sv_2mortal(newRV_noinc((SV *) newAV())), NULL, NULL );
	EXEC_REQUEST(self, ctxsv, ctx, iid, pkt, NULL);
	TIMEOUT_TIMER(self, iid, self->cnn.rw_timeout);
}

#define on_read_no_body(self, len)\
	do_disable_rw_timer(self);\
	\
	TntCnn * tnt = (TntCnn *) self;\
	char *rbuf = self->rbuf;\
	char *end = rbuf + self->ruse;\
	\
	/* len */\
	ptrdiff_t buf_len = end - rbuf;\
	if (buf_len < 5) {\
		debug("not enough");\
		return;\
	}\
	\
	uint32_t pkt_length;\
	decode_pkt_len_(&rbuf, pkt_length);\
	\
	if (buf_len - 5 < pkt_length) {\
		debug("not enough");\
		return;\
	}\
	\
	HV *hv = (HV *) sv_2mortal((SV *) newHV());\
	\
	/* header */\
	uint32_t id = 0;\
	int length = parse_reply_hdr(hv, rbuf, buf_len, &id);\
	if (unlikely(length < 0)) {\
		TNT_CROAK("Unexpected response header");\
		return;\
	}\
	if (unlikely(id <= 0)) {\
		PE_CROAK("Wrong sync id (id <= 0)");\
		rbuf += length;\
		return;\
	}\
	\
	TntCtx *ctx;\
	{\
		SV *key = hv_delete(tnt->reqs, (char *) &id, sizeof(id), 0);\
		\
		if (!key) {\
			cwarn("key %d not found", id);\
			rbuf += length;\
			self->ruse = end - rbuf;\
			if (self->ruse > 0) {\
				memmove(self->rbuf,rbuf,self->ruse);\
			}\
			\
			FREETMPS;\
			LEAVE;\
			return;\
		} else {\
			ctx = (TntCtx *) SvPVX(key);\
			ev_timer_stop(self->loop, &ctx->t);\
			SvREFCNT_dec(ctx->wbuf);\
			if (ctx->f.size && !ctx->f.nofree) {\
				safefree(ctx->f.f);\
			}\
		}\
	}\
	rbuf += length;

static void on_read(ev_cnn * self, size_t len) {
	ENTER;
	SAVETMPS;

	do_disable_rw_timer(self);

	TntCnn * tnt = (TntCnn *) self;
	char *rbuf = self->rbuf;
	char *end = rbuf + self->ruse;

	while ( rbuf < end ) {
		/* len */
		ptrdiff_t buf_len = end - rbuf;
		if (buf_len < 5) {
			debug("not enough");
			return;
		}

		uint32_t pkt_length;
		decode_pkt_len_(&rbuf, pkt_length);

		if (buf_len - 5 < pkt_length) {
			debug("not enough");
			return;
		}

		HV *hv = (HV *) sv_2mortal((SV *) newHV());

		/* header */
		uint32_t id = 0;
		int length = parse_reply_hdr(hv, rbuf, buf_len, &id);
		if (unlikely(length < 0)) {
			TNT_CROAK("Unexpected response header");
			return;
		}
		if (unlikely(id <= 0)) {
			PE_CROAK("Wrong sync id (id <= 0)");
			rbuf += length;
			return;
		}

		TntCtx *ctx;
		SV *key = hv_delete(tnt->reqs, (char *) &id, sizeof(id), 0);

		if (!key) {
			cwarn("key %d not found", id);
			rbuf += pkt_length;

		} else {
			ctx = (TntCtx *) SvPVX(key);
			ev_timer_stop(self->loop, &ctx->t);
			SvREFCNT_dec(ctx->wbuf);
			if (ctx->f.size && !ctx->f.nofree) {
				safefree(ctx->f.f);
			}
			rbuf += length;

			/* body */

			AV *fields = (ctx->space && ctx->use_hash) ? ctx->space->fields : NULL;
			length = parse_reply_body(hv, rbuf, buf_len, &ctx->f, fields);
			if (unlikely(length <= 0)) {
				TNT_CROAK("Unexpected response body");
				return;
			}
			rbuf += length;

			dSP;

			if (ctx->cb) {
				SPAGAIN;

				ENTER; SAVETMPS;

				SV ** var = hv_fetchs(hv,"code",0);
				if (var && SvIV (*var) == 0) {
					PUSHMARK(SP);
					EXTEND(SP, 1);
					PUSHs( sv_2mortal(newRV_noinc( SvREFCNT_inc((SV *) hv) )) );
					PUTBACK;
				}
				else {
					var = hv_fetchs(hv,"errstr",0);
					PUSHMARK(SP);
					EXTEND(SP, 3);
					PUSHs( &PL_sv_undef );
					PUSHs( var && *var ? sv_2mortal(newSVsv(*var)) : &PL_sv_undef );
					PUSHs( sv_2mortal(newRV_noinc( SvREFCNT_inc((SV *) hv) )) );
					PUTBACK;
				}

				(void) call_sv(ctx->cb, G_DISCARD | G_VOID);

				//SPAGAIN;PUTBACK;

				SvREFCNT_dec(ctx->cb);

				FREETMPS; LEAVE;
			}


			--tnt->pending;

			if (rbuf == end) {
				self->ruse = 0;
				if (tnt->pending == 0) {
					//do_disable_rw_timer(self);
				}
				else {
					//do_enable_rw_timer(self);
				}
				break;
			}
		}

	}

	self->ruse = end - rbuf;
	if (self->ruse > 0) {
		memmove(self->rbuf,rbuf,self->ruse);
	}

	FREETMPS;
	LEAVE;
}

static void on_index_info_read(ev_cnn * self, size_t len) {
	ENTER;
	SAVETMPS;

	on_read_no_body(self, len);

	/* body */

	length = parse_index_body(tnt->spaces, hv, rbuf, buf_len);
	if (unlikely(length <= 0)) {
		TNT_CROAK("Unexpected response body");
		return;
	}
	rbuf += length;

	self->on_read = (c_cb_read_t) on_read;


	SV **key = hv_fetchs(hv, "code", 0);
	if (key && *key && SvIOK(*key) && SvIV(*key) == 0) {
		call_connected(tnt);
	} else {
		force_disconnect(tnt, "Couldn\'t retrieve index info.");
	}

	--tnt->pending;

	self->ruse = end - rbuf;
	if (self->ruse > 0) {
		//cwarn("move buf on %zu",self->ruse);
		memmove(self->rbuf,rbuf,self->ruse);
	}

	FREETMPS;
	LEAVE;
}

static void on_spaces_info_read(ev_cnn * self, size_t len) {
	ENTER;
	SAVETMPS;

	on_read_no_body(self, len);

	/* body */

	length = parse_spaces_body(hv, rbuf, buf_len);
	if (unlikely(length <= 0)) {
		TNT_CROAK("Unexpected response body");
		return;
	}
	rbuf += length;

	SV **key = hv_fetchs(hv, "code", 0);
	if (unlikely(!key || !(*key) || !SvIOK(*key) || SvIV(*key) != 0)) {
		force_disconnect(tnt, "Couldn\'t retrieve space info.");
	} else {
		if ((key = hv_fetchs(hv, "data", 0)) && SvOK(*key) && SvROK(*key)) {
			self->on_read = (c_cb_read_t) on_index_info_read;

			if (tnt->spaces) {
				destroy_spaces(tnt->spaces);
			}
			tnt->spaces = (HV *) SvREFCNT_inc(SvRV(*key));

			_execute_select(tnt, _INDEX_SPACEID);
			// self->on_read = (c_cb_read_t) on_read;
		} else {
			force_disconnect(tnt, "Couldn\'t retrieve space info.");
		}
	}

	--tnt->pending;

	self->ruse = end - rbuf;
	if (self->ruse > 0) {
		memmove(self->rbuf,rbuf,self->ruse);
	}

	FREETMPS;
	LEAVE;
}

static void on_auth_read(ev_cnn * self, size_t len) {
	ENTER;
	SAVETMPS;

	on_read_no_body(self, len);

	/* body */

	AV *fields = (ctx->space && ctx->use_hash) ? ctx->space->fields : NULL;
	length = parse_reply_body(hv, rbuf, buf_len, &ctx->f, fields);
	if (unlikely(length <= 0)) {
		TNT_CROAK("Unexpected response body");
		return;
	}
	rbuf += length;


	SV ** var = hv_fetchs(hv,"code",0);
	if (var && SvIV (*var) == 0) {
		self->on_read = (c_cb_read_t) on_spaces_info_read;
		_execute_select(tnt, _SPACE_SPACEID);
	}
	else {
		var = hv_fetchs(hv,"errstr",0);
		force_disconnect(tnt, SvPVX(*var));
	}

	--tnt->pending;

	self->ruse = end - rbuf;
	if (self->ruse > 0) {
		memmove(self->rbuf,rbuf,self->ruse);
	}

	FREETMPS;
	LEAVE;
}

static void on_greet_read(ev_cnn * self, size_t len) {

	ENTER;
	SAVETMPS;

	do_disable_rw_timer(self);

	TntCnn * tnt = (TntCnn *) self;
	char *rbuf = self->rbuf;
	char *end = rbuf + self->ruse;

	ptrdiff_t buf_len = end - rbuf;
	if (buf_len < 128) {
		return;
	}

	char *tnt_ver_begin = NULL, *tnt_ver_end = NULL;
	char *salt_begin = NULL, *salt_end = NULL;
	decode_greeting(rbuf, tnt_ver_begin, tnt_ver_end, salt_begin, salt_end);
	warn("%.*s", (int) (tnt_ver_end - tnt_ver_begin), tnt_ver_begin);

	self->ruse -= buf_len;
	if (self->ruse > 0) {
		//cwarn("move buf on %zu",self->ruse);
		memmove(self->rbuf,rbuf,self->ruse);
	}

	if (tnt->username && SvOK(tnt->username) && SvPOK(tnt->username) && tnt->password && SvOK(tnt->password) && SvPOK(tnt->password)) {
		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		ctx->self = self;
		ctx->call = "auth";
		ctx->use_hash = tnt->use_hash;
		uint32_t iid = ++tnt->seq;
		ctx->id = iid;
		SV *pkt = pkt_authenticate(iid, tnt->username, tnt->password, salt_begin, salt_end, NULL);

		self->on_read = (c_cb_read_t) on_auth_read;
		EXEC_REQUEST(tnt, ctxsv, ctx, iid, pkt, NULL);
		TIMEOUT_TIMER(tnt, iid, tnt->cnn.rw_timeout);
	} else {
		self->on_read = (c_cb_read_t) on_spaces_info_read;
		_execute_select(tnt, _SPACE_SPACEID);
		// self->on_read = (c_cb_read_t) on_read;
	}

	FREETMPS;
	LEAVE;
}

void free_reqs (TntCnn *self, const char * message) {

	ENTER;SAVETMPS;

	dSP;

	HE *ent;
	(void) hv_iterinit( self->reqs );
	while ((ent = hv_iternext( self->reqs ))) {
		TntCtx * ctx = (TntCtx *) SvPVX( HeVAL(ent) );
		ev_timer_stop(self->cnn.loop,&ctx->t);
		SvREFCNT_dec(ctx->wbuf);
		if (ctx->f.size && !ctx->f.nofree) {
			safefree(ctx->f.f);
		}

		if (ctx->cb) {
			SPAGAIN;
			ENTER; SAVETMPS;

			PUSHMARK(SP);
			EXTEND(SP, 2);
			PUSHs( &PL_sv_undef );
			PUSHs( sv_2mortal(newSVpvf("%s", message)) );
			PUTBACK;

			(void) call_sv( ctx->cb, G_DISCARD | G_VOID );

			//SPAGAIN;PUTBACK;

			SvREFCNT_dec(ctx->cb);

			FREETMPS; LEAVE;
		}

		--self->pending;
	}

	hv_clear(self->reqs);

	FREETMPS;LEAVE;
}


static void on_disconnect (TntCnn * self, int err) {
	ENTER;SAVETMPS;

	//warn("disconnect: %s", strerror(err));
	if (err == 0) {
		free_reqs(self, "Connection closed");
	} else {
		SV *msg = sv_2mortal(newSVpvf("Disconnected: %s",strerror(err)));
		free_reqs(self, SvPVX(msg));
	}

	self->cnn.on_read = (c_cb_read_t) on_greet_read;

	FREETMPS;LEAVE;
}

INLINE SV *get_bool(const char *name) {
	SV *sv = get_sv(name, 1);

	SvREADONLY_on(sv);
	SvREADONLY_on(SvRV (sv));

	return sv;
}


MODULE = EV::Tarantool      PACKAGE = EV::Tarantool::DES

void DESTROY(SV *this)
	PPCODE:
		cwarn("DESTROY %p -> %p (%d)",this,SvRV(this),SvREFCNT( SvRV(this) ));

MODULE = EV::Tarantool      PACKAGE = EV::Tarantool
PROTOTYPES: DISABLE
BOOT:
{
	I_EV_API ("EV::Tarantool");
	I_EV_CNN_API("EV::Tarantool");

	types_boolean_stash = gv_stashpv("Types::Serialiser::Boolean", 1);

	types_true  = get_bool("Types::Serialiser::true");
	types_false = get_bool("Types::Serialiser::false");
}


void new(SV *pk, HV *conf)
	PPCODE:
		if (0) pk = pk;
		xs_ev_cnn_new(TntCnn); // declares YourType * self, set ST(0) // TODO: connected cb is assigned here, but it shoudldn't however
		self->default_on_connected_cb = self->cnn.on_connected;
		self->cnn.on_connected = (c_cb_conn_t) tnt_on_connected_cb;
		self->cnn.on_read = (c_cb_read_t) on_greet_read;
		// self->cnn.on_read = (c_cb_read_t) on_read;
		self->on_disconnect_before = (c_cb_err_t) on_disconnect;


		//cwarn("new     this: %p; iv[%d]: %p; self: %p; self->self: %p",ST(0), SvREFCNT(iv),iv, self, self->self);

		SV **key;

		self->reqs = newHV();

		self->use_hash = 1;
		if ((key = hv_fetchs(conf, "hash", 0)) ) self->use_hash = SvOK(*key) ? SvIV(*key) : 0;
		if ((key = hv_fetchs(conf, "username", 0)) && SvPOK(*key)) SvREFCNT_inc(self->username = *key);
		if ((key = hv_fetchs(conf, "password", 0)) && SvPOK(*key)) SvREFCNT_inc(self->password = *key);

		self->spaces = newHV();

		// if ((key = hv_fetchs(conf, "spaces", 0)) && SvROK(*key)) {
			// configure_spaces( self->spaces, *key );
		// }
		XSRETURN(1);


void DESTROY(SV *this)
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);

		//cwarn("destroy this: %p; iv[%d]: %p; self: %p; self->self: %p",ST(0), SvREFCNT(SvRV(this)), SvRV(this), self, self->self);
		//SV * leak = newSV(1024);
		if (!PL_dirty) {
			if (self->reqs) {
				free_reqs(self, "Destroyed");
				SvREFCNT_dec(self->reqs);
			}
			if (self->spaces) {
				destroy_spaces(self->spaces);
			}
		}
		if (self->username) SvREFCNT_dec(self->username);
		if (self->password) SvREFCNT_dec(self->password);
		xs_ev_cnn_destroy(self);


void reqs(SV *this)
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);
		ST(0) = sv_2mortal(newRV_inc((SV *)self->reqs));
		XSRETURN(1);


void spaces(SV *this)
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);
		ST(0) = sv_2mortal(newRV_inc((SV *)self->spaces));
		XSRETURN(1);


void ping(SV *this, ... )
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn(self,cb);

		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		ctx->self = self;
		ctx->call = "ping";
		ctx->use_hash = self->use_hash;
		uint32_t iid = ++self->seq;
		ctx->id = iid;
		SV *pkt = pkt_ping(iid);
		HV *opts = items == 3 ? (HV *) SvRV(ST( 1 )) : 0;
		EXEC_REQUEST(self, ctxsv, ctx, iid, pkt, cb);
		INIT_TIMEOUT_TIMER(self, iid, opts);

		XSRETURN_UNDEF;


void select( SV *this, SV *space, SV * keys, ... )
	PPCODE:
		if (0) this = this;
		// TODO: croak cleanup may be solved with refcnt+mortal
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn(self,cb);

		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		ctx->self = self;
		ctx->call = "select";
		ctx->use_hash = self->use_hash;
		uint32_t iid = ++self->seq;
		ctx->id = iid;
		HV *opts = items == 5 ? (HV *) SvRV(ST( 3 )) : 0;
		SV *pkt = pkt_select(ctx, iid, self->spaces, space, keys, opts, cb );
		EXEC_REQUEST(self, ctxsv, ctx, iid, pkt, cb);
		INIT_TIMEOUT_TIMER(self, iid, opts);

		XSRETURN_UNDEF;


void insert( SV *this, SV *space, SV * t, ... )
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn(self,cb);

		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		ctx->self = self;
		ctx->call = "insert";
		ctx->use_hash = self->use_hash;
		uint32_t iid = ++self->seq;
		ctx->id = iid;
		HV *opts = items == 5 ? (HV *) SvRV(ST( 3 )) : 0;
		SV *pkt = pkt_insert(ctx, iid, self->spaces, space, t, opts, cb );
		EXEC_REQUEST(self, ctxsv, ctx, iid, pkt, cb);
		INIT_TIMEOUT_TIMER(self, iid, opts);

		XSRETURN_UNDEF;


void update( SV *this, SV *space, SV * key, SV * tuple, ... )
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn(self,cb);

		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		ctx->self = self;
		ctx->call = "update";
		ctx->use_hash = self->use_hash;
		uint32_t iid = ++self->seq;
		ctx->id = iid;
		HV *opts = items == 6 ? (HV *) SvRV(ST(4)) : 0;
		SV *pkt = pkt_update(ctx, iid, self->spaces, space, key, tuple, opts, cb );
		EXEC_REQUEST(self, ctxsv, ctx, iid, pkt, cb);
		INIT_TIMEOUT_TIMER(self, iid, opts);

		XSRETURN_UNDEF;


void delete( SV *this, SV *space, SV * t, ... )
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn(self,cb);

		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		ctx->self = self;
		ctx->call = "delete";
		ctx->use_hash = self->use_hash;
		uint32_t iid = ++self->seq;
		ctx->id = iid;
		HV *opts = items == 5 ? (HV *) SvRV(ST( 3 )) : 0;
		SV *pkt = pkt_delete(ctx, iid, self->spaces, space, t, opts, cb );
		EXEC_REQUEST(self, ctxsv, ctx, iid, pkt, cb);
		INIT_TIMEOUT_TIMER(self, iid, opts);

		XSRETURN_UNDEF;


void eval( SV *this, SV *expression, SV * t, ... )
	PPCODE:
		if (0) this = this;
		// TODO: croak cleanup may be solved with refcnt+mortal
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn(self,cb);

		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		ctx->self = self;
		ctx->call = "eval";
		ctx->use_hash = self->use_hash;
		uint32_t iid = ++self->seq;
		ctx->id = iid;
		HV *opts = items == 5 ? (HV *) SvRV(ST( 3 )) : 0;
		SV *pkt = pkt_eval(ctx, iid, self->spaces, expression, t, opts, cb );
		EXEC_REQUEST(self, ctxsv, ctx, iid, pkt, cb);
		INIT_TIMEOUT_TIMER(self, iid, opts);

		XSRETURN_UNDEF;


void call( SV *this, SV *function_name, SV * t, ... )
	PPCODE:
		if (0) this = this;
		// TODO: croak cleanup may be solved with refcnt+mortal
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn(self,cb);

		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		ctx->self = self;
		ctx->call = "call";
		ctx->use_hash = self->use_hash;
		uint32_t iid = ++self->seq;
		ctx->id = iid;
		HV *opts = items == 5 ? (HV *) SvRV(ST( 3 )) : 0;
		SV *pkt = pkt_call(ctx, iid, self->spaces, function_name, t, opts, cb );
		EXEC_REQUEST(self, ctxsv, ctx, iid, pkt, cb);
		INIT_TIMEOUT_TIMER(self, iid, opts);

		XSRETURN_UNDEF;
