#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "ppport.h"
#include "EVAPI.h"


#define MYDEBUG
#define XSEV_CON_HOOKS 1

#include "log.h"
#include "xsevcnn.h"
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

#ifndef TNT_WBUF_LIMIT
#  define TNT_WBUF_LIMIT 16384
#endif

typedef struct {
	xs_ev_cnn_struct;


	c_cb_discon_t on_disconnect_before;
	c_cb_discon_t on_disconnect_after;
	c_cb_conn_t on_connect_before;
	c_cb_conn_t on_connect_after;

	c_cb_conn_t default_on_connected_cb;
	struct sockaddr peer_info;

	uint32_t pending;
	uint32_t seq;
	U32      use_hash;
	HV      *reqs;
	HV      *spaces;
	SV      *username;
	SV      *password;
	uint8_t  log_level;
	uint32_t wbuf_limit;
} TntCnn;

// static const uint32_t _SPACE_SPACEID = 280;
// static const uint32_t _INDEX_SPACEID = 288;

static const char *_SPACE_SELECTOR = "return unpack(box.space._space:select{})";
static const char *_INDEX_SELECTOR = "return unpack(box.space._index:select{})";
static const size_t SELECTOR_STR_LENGTH = 40;

void tnt_on_connected_cb(ev_cnn *cnn, struct sockaddr *peer) {
	if (likely(peer != NULL)) {
		TntCnn *self = (TntCnn *) cnn;
		self->peer_info = *peer;
	}
}

INLINE void call_connected(TntCnn *self) {
	self->default_on_connected_cb(&self->cnn, &self->peer_info);
}

INLINE void force_disconnect(TntCnn *self, const char *reason) {

	on_connect_reset(&self->cnn, 0, reason);
}

static void on_request_timer(EV_P_ ev_timer *t, int flags) {
	TntCtx *ctx = (TntCtx *) t;
	TntCnn *self = (TntCnn *) ctx->self;
	log_warn(self->log_level, "timer called on %p: %s", ctx, ctx->call);
	ENTER;SAVETMPS;
	dSP;

	(void) hv_delete( self->reqs, (char *) &ctx->id, sizeof(ctx->id),0);

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

	--self->pending;

	FREETMPS;LEAVE;
}

#define TIMEOUT_TIMER(self, ctx, iid, timeout) STMT_START { \
	if (timeout > 0) { \
		ev_timer_init(&ctx->t, on_request_timer, timeout, 0.); \
		ev_timer_start(self->cnn.loop, &ctx->t); \
	} \
} STMT_END

#define INIT_TIMEOUT_TIMER(self, ctx, iid, opts) STMT_START { \
	double timeout; \
	SV **key; \
	\
	if ( opts && (key = hv_fetchs( opts, "timeout", 0 ))) { \
		timeout = SvNV( *key ); \
		/*cwarn("timeout set: %f",timeout);*/\
	} else { \
		timeout = self->cnn.rw_timeout; \
	} \
	TIMEOUT_TIMER(self, ctx, iid, timeout); \
} STMT_END

#define __EXEC_REQUEST(self, ctxsv, ctx, iid, _cb) STMT_START { \
	SvREFCNT_inc(ctx->cb = (_cb)); \
	(void) hv_store( self->reqs, (char *)&iid, sizeof(iid), SvREFCNT_inc(ctxsv), 0 ); \
	++self->pending; \
	do_write(&self->cnn,SvPVX(ctx->wbuf), SvCUR(ctx->wbuf)); \
} STMT_END

#define EXEC_REQUEST_TIMEOUT(self, ctxsv, ctx, iid, pkt, opts, _cb) STMT_START { \
	if ((ctx->wbuf = pkt)) { \
		__EXEC_REQUEST(self, ctxsv, ctx, iid, _cb); \
		INIT_TIMEOUT_TIMER(self, ctx, iid, opts); \
	} \
} STMT_END

#define EXEC_REQUEST(self, ctxsv, ctx, iid, pkt, _cb) STMT_START { \
	if ((ctx->wbuf = pkt)) { \
		__EXEC_REQUEST(self, ctxsv, ctx, iid, _cb); \
	} \
} STMT_END

#define INIT_CTX(_self, ctx, method, iid) STMT_START { \
	ctx->self = _self; \
	ctx->call = method; \
	ctx->use_hash = _self->use_hash; \
	ctx->log_level = _self->log_level; \
	iid = ++_self->seq; \
	ctx->id = iid; \
} STMT_END

#define croak_cb_xsundef(cb, ...) STMT_START { \
	_croak_cb(cb, __VA_ARGS__); \
	XSRETURN_UNDEF; \
	return; \
} STMT_END

#define GET_OPTS(OPTS_NAME, opts_sv, cb) STMT_START { \
	SV *_opts_sv = (opts_sv); \
	OPTS_NAME = NULL; \
	if (_opts_sv != NULL) { \
		if (likely(SvROK(_opts_sv) && SvTYPE(SvRV(_opts_sv)) == SVt_PVHV)) { \
			OPTS_NAME = (HV *) SvRV(_opts_sv); \
		} else if (_opts_sv != &PL_sv_undef) { \
			croak_cb_xsundef(cb, "Opts must be a HASHREF"); \
		} \
	} \
} STMT_END

INLINE void _execute_eval(TntCnn *self, const char *expr) {
	dSVX(ctxsv, ctx, TntCtx);
	sv_2mortal(ctxsv);
	uint32_t iid;

	INIT_CTX(self, ctx, "eval", iid);
	SV *pkt = pkt_eval(ctx, iid, self->spaces, sv_2mortal(newSVpvn(expr, SELECTOR_STR_LENGTH)), sv_2mortal(newRV_noinc((SV *) newAV())), NULL, NULL);
	EXEC_REQUEST(self, ctxsv, ctx, iid, pkt, NULL);


	// INIT_CTX(self, ctx, "select", iid);
	// SV *pkt = pkt_select(ctx, iid, self->spaces, sv_2mortal(newSVuv(space_id)), sv_2mortal(newRV_noinc((SV *) newAV())), NULL, NULL );
	// EXEC_REQUEST(self, ctxsv, ctx, iid, pkt, NULL);
	TIMEOUT_TIMER(self, ctx, iid, self->cnn.rw_timeout);
}


static void on_read(ev_cnn *self, size_t len) {
	ENTER;
	SAVETMPS;

	do_disable_rw_timer(self);

	TntCnn *tnt = (TntCnn *) self;
	char *rbuf = self->rbuf;
	char *end = rbuf + self->ruse;
	
	dSP;

	while ( rbuf < end ) {
		/* len */
		ptrdiff_t buf_len = end - rbuf;
		if (buf_len < 5) {
			//cwarn("buf_len < 5");
			debug("not enough");
			break;
		}

		uint32_t pkt_length;
		decode_pkt_len_(&rbuf, pkt_length);

		if (buf_len - 5 < pkt_length) {
			//cwarn("not enough for a packet");
			debug("not enough");
			break;
		}
		rbuf += 5;

		HV *hv = (HV *) sv_2mortal((SV *) newHV());

		/* header */
		tnt_header_t hdr;
		int hdr_length = parse_reply_hdr(hv, rbuf, buf_len, &hdr, tnt->log_level);
		if (unlikely(hdr_length < 0)) {
			TNT_CROAK("Unexpected response header");
			return;
		}
		if (unlikely(hdr.id <= 0)) {
			PE_CROAK("Wrong sync id (id <= 0)");
			return;
		}

		TntCtx *ctx;
		SV *key = hv_delete(tnt->reqs, (char *) &hdr.id, sizeof(hdr.id), 0);

		if (!key) {
			rbuf += pkt_length;
			log_debug(tnt->log_level, "key %d not found", hdr.id);
		} else {
			rbuf += hdr_length;

			ctx = (TntCtx *) SvPVX(key);
			ev_timer_stop(self->loop, &ctx->t);
			SvREFCNT_dec(ctx->wbuf);
			if (ctx->f.size && !ctx->f.nofree) {
				safefree(ctx->f.f);
			}

			/* body */

			AV *fields = (ctx->space && ctx->use_hash) ? ctx->space->fields : NULL;
			int body_length = parse_reply_body(ctx, hv, rbuf, buf_len, &ctx->f, fields);
			if (unlikely(body_length <= 0)) {
				rbuf += (pkt_length - hdr_length);
				log_error(tnt->log_level, "Unexpected response body. length = %d", body_length);
			} else {
				rbuf += body_length;
			}

			if (ctx->cb) {
				SPAGAIN;

				ENTER; SAVETMPS;

				SV **var = NULL;
				if (hdr.code == 0) {
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

static void on_index_info_read(ev_cnn *self, size_t len) {
	ENTER;
	SAVETMPS;

	do_disable_rw_timer(self);

	TntCnn *tnt = (TntCnn *) self;
	char *rbuf = self->rbuf;
	char *end = rbuf + self->ruse;

	while ( rbuf < end ) {
		/* len */
		ptrdiff_t buf_len = end - rbuf;
		if (buf_len < 5) {
			//cwarn("buf_len < 5");
			debug("not enough");
			break;
		}

		uint32_t pkt_length;
		decode_pkt_len_(&rbuf, pkt_length);

		if (buf_len - 5 < pkt_length) {
			//cwarn("not enough for a packet");
			debug("not enough");
			break;
		}
		rbuf += 5;

		HV *hv = (HV *) sv_2mortal((SV *) newHV());

		/* header */
		tnt_header_t hdr;
		int hdr_length = parse_reply_hdr(hv, rbuf, buf_len, &hdr, tnt->log_level);
		if (unlikely(hdr_length < 0)) {
			TNT_CROAK("Unexpected response header");
			return;
		}
		if (unlikely(hdr.id <= 0)) {
			PE_CROAK("Wrong sync id (id <= 0)");
			return;
		}

		TntCtx *ctx;
		SV *key = hv_delete(tnt->reqs, (char *) &hdr.id, sizeof(hdr.id), 0);

		if (!key) {
			rbuf += pkt_length;
			log_debug(tnt->log_level, "key %d not found", hdr.id);
		} else {
			rbuf += hdr_length;

			ctx = (TntCtx *) SvPVX(key);
			ev_timer_stop(self->loop, &ctx->t);
			SvREFCNT_dec(ctx->wbuf);
			if (ctx->f.size && !ctx->f.nofree) {
				safefree(ctx->f.f);
			}

			if (unlikely(hdr.code != 0)) {
				log_error(tnt->log_level, "Failed to retrieve index info. Code = %d", (int) hdr.code);
				force_disconnect(tnt, "Couldn\'t retrieve index info.");
			} else {

				/* body */

				int body_length = parse_index_body(tnt->spaces, hv, rbuf, buf_len, tnt->log_level);
				if (unlikely(body_length <= 0)) {
					rbuf += (pkt_length - hdr_length);
					log_error(tnt->log_level, "Unexpected response body. length = %d", body_length);
					force_disconnect(tnt, "Couldn\'t retrieve index info.");
				} else {
					rbuf += body_length;

					self->on_read = (c_cb_read_t) on_read;
					call_connected(tnt);
				}
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

static void on_spaces_info_read(ev_cnn *self, size_t len) {
	ENTER;
	SAVETMPS;

	do_disable_rw_timer(self);

	TntCnn *tnt = (TntCnn *) self;
	char *rbuf = self->rbuf;
	char *end = rbuf + self->ruse;

	while ( rbuf < end ) {
		/* len */
		ptrdiff_t buf_len = end - rbuf;
		if (buf_len < 5) {
			//cwarn("buf_len < 5");
			debug("not enough");
			break;
		}

		uint32_t pkt_length;
		decode_pkt_len_(&rbuf, pkt_length);

		if (buf_len - 5 < pkt_length) {
			//cwarn("not enough for a packet");
			debug("not enough");
			break;
		}
		rbuf += 5;

		HV *hv = (HV *) sv_2mortal((SV *) newHV());

		/* header */
		tnt_header_t hdr;
		int hdr_length = parse_reply_hdr(hv, rbuf, buf_len, &hdr, tnt->log_level);
		if (unlikely(hdr_length < 0)) {
			TNT_CROAK("Unexpected response header");
			return;
		}
		if (unlikely(hdr.id <= 0)) {
			PE_CROAK("Wrong sync id (id <= 0)");
			return;
		}

		TntCtx *ctx;
		SV *key = hv_delete(tnt->reqs, (char *) &hdr.id, sizeof(hdr.id), 0);

		if (!key) {
			rbuf += pkt_length;
			log_debug(tnt->log_level, "key %d not found", hdr.id);
		} else {
			rbuf += hdr_length;

			ctx = (TntCtx *) SvPVX(key);
			ev_timer_stop(self->loop, &ctx->t);
			SvREFCNT_dec(ctx->wbuf);
			if (ctx->f.size && !ctx->f.nofree) {
				safefree(ctx->f.f);
			}

			SV **key = NULL;
			if (unlikely(hdr.code != 0)) {
				log_error(tnt->log_level, "Couldn\'t retrieve space info. Code = %d", (int) hdr.code);
				force_disconnect(tnt, "Couldn\'t retrieve space info");
			} else {

				/* body */
				
				int body_length = parse_spaces_body(hv, rbuf, buf_len, tnt->log_level);
				if (unlikely(body_length <= 0)) {
					rbuf += (pkt_length - hdr_length);
					log_error(tnt->log_level, "Unexpected response body. length = %d", body_length);
					force_disconnect(tnt, "Couldn\'t retrieve space info.");
				} else {
					rbuf += body_length;

					if ((key = hv_fetchs(hv, "data", 0)) && SvOK(*key) && SvROK(*key)) {
						if (tnt->spaces) {
							destroy_spaces(tnt->spaces);
						}
						tnt->spaces = (HV *) SvREFCNT_inc(SvRV(*key));

						self->on_read = (c_cb_read_t) on_index_info_read;
						_execute_eval(tnt, _INDEX_SELECTOR);
						// self->on_read = (c_cb_read_t) on_read;
					} else {
						log_error(tnt->log_level, "Couldn\'t retrieve space info. No data parsed");
						force_disconnect(tnt, "Couldn\'t retrieve space info.");
					}
				}
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

static void on_auth_read(ev_cnn *self, size_t len) {
	ENTER;
	SAVETMPS;

	do_disable_rw_timer(self);

	TntCnn *tnt = (TntCnn *) self;
	char *rbuf = self->rbuf;
	char *end = rbuf + self->ruse;

	while ( rbuf < end ) {
		/* len */
		ptrdiff_t buf_len = end - rbuf;
		if (buf_len < 5) {
			//cwarn("buf_len < 5");
			debug("not enough");
			break;
		}

		uint32_t pkt_length;
		decode_pkt_len_(&rbuf, pkt_length);

		if (buf_len - 5 < pkt_length) {
			//cwarn("not enough for a packet");
			debug("not enough");
			break;
		}
		rbuf += 5;

		HV *hv = (HV *) sv_2mortal((SV *) newHV());

		/* header */
		tnt_header_t hdr;
		int hdr_length = parse_reply_hdr(hv, rbuf, buf_len, &hdr, tnt->log_level);
		if (unlikely(hdr_length < 0)) {
			TNT_CROAK("Unexpected response header");
			return;
		}
		if (unlikely(hdr.id <= 0)) {
			PE_CROAK("Wrong sync id (id <= 0)");
			return;
		}

		TntCtx *ctx;
		SV *key = hv_delete(tnt->reqs, (char *) &hdr.id, sizeof(hdr.id), 0);

		if (!key) {
			rbuf += pkt_length;
			log_debug(tnt->log_level, "key %d not found", hdr.id);
		} else {
			rbuf += hdr_length;

			ctx = (TntCtx *) SvPVX(key);
			ev_timer_stop(self->loop, &ctx->t);
			SvREFCNT_dec(ctx->wbuf);
			if (ctx->f.size && !ctx->f.nofree) {
				safefree(ctx->f.f);
			}

			/* body */

			AV *fields = (ctx->space && ctx->use_hash) ? ctx->space->fields : NULL;
			int body_length = parse_reply_body(ctx, hv, rbuf, buf_len, &ctx->f, fields);
			if (unlikely(body_length <= 0)) {
				rbuf += (pkt_length - hdr_length);
				log_error(tnt->log_level, "Unexpected response body. length = %d", body_length);
				force_disconnect(tnt, "Couldn\'t authenticate.");
			} else {
				rbuf += body_length;

				SV **var = NULL;
				if (hdr.code == 0) {
					self->on_read = (c_cb_read_t) on_spaces_info_read;
					_execute_eval(tnt, _SPACE_SELECTOR);
					// self->on_read = (c_cb_read_t) on_read;
				}
				else {
					var = hv_fetchs(hv,"errstr",0);
					force_disconnect(tnt, SvPVX(*var));
				}
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

static void on_greet_read(ev_cnn *self, size_t len) {

	ENTER;
	SAVETMPS;

	do_disable_rw_timer(self);

	TntCnn *tnt = (TntCnn *) self;
	char *rbuf = self->rbuf;
	char *end = rbuf + self->ruse;

	ptrdiff_t buf_len = end - rbuf;
	if (buf_len < 128) {
		return;
	}

	char *tnt_ver_begin = NULL, *tnt_ver_end = NULL;
	char *salt_begin = NULL, *salt_end = NULL;
	decode_greeting(rbuf, tnt_ver_begin, tnt_ver_end, salt_begin, salt_end);
	log_info(tnt->log_level, "%.*s", (int) (tnt_ver_end - tnt_ver_begin), tnt_ver_begin);

	self->ruse -= buf_len;
	if (self->ruse > 0) {
		//cwarn("move buf on %zu",self->ruse);
		memmove(self->rbuf,rbuf,self->ruse);
	}

	if (tnt->username && SvOK(tnt->username) && SvPOK(tnt->username) && tnt->password && SvOK(tnt->password) && SvPOK(tnt->password)) {
		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		uint32_t iid;
		INIT_CTX(tnt, ctx, "auth", iid);
		SV *pkt = pkt_authenticate(iid, tnt->username, tnt->password, salt_begin, salt_end, NULL);

		self->on_read = (c_cb_read_t) on_auth_read;
		EXEC_REQUEST(tnt, ctxsv, ctx, iid, pkt, NULL);
		TIMEOUT_TIMER(tnt, ctx, iid, tnt->cnn.rw_timeout);
	} else {
		self->on_read = (c_cb_read_t) on_spaces_info_read;
		_execute_eval(tnt, _SPACE_SELECTOR);
		// self->on_read = (c_cb_read_t) on_read;
		// call_connected(tnt);
	}

	FREETMPS;
	LEAVE;
}

void free_reqs (TntCnn *self, const char *message) {
	if (unlikely(!self->reqs)) return;

	ENTER;SAVETMPS;

	dSP;

	HE *ent;
	(void) hv_iterinit( self->reqs );
	while ((ent = hv_iternext( self->reqs ))) {
		TntCtx *ctx = (TntCtx *) SvPVX( HeVAL(ent) );
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


static void on_disconnect (TntCnn *self, int err, const char *reason) {
	ENTER;SAVETMPS;

	if (err == 0) {
		free_reqs(self, "Connection closed");
	} else {
		SV *msg = sv_2mortal(newSVpvf("Disconnected: %s",strerror(err)));
		free_reqs(self, SvPVX(msg));
	}

	if (self->spaces) {
		destroy_spaces(self->spaces);
		self->spaces = NULL;
	}

	self->cnn.on_read = (c_cb_read_t) on_greet_read;

	FREETMPS;LEAVE;
}

INLINE SV *get_bool(const char *name) {
	SV *sv = get_sv(name, 1);

	SvREADONLY_on(sv);
	SvREADONLY_on(SvRV(sv));

	return sv;
}


MODULE = EV::Tarantool16      PACKAGE = EV::Tarantool16
PROTOTYPES: DISABLE
BOOT:
{
	I_EV_API ("EV::Tarantool16");
	I_EV_CNN_API("EV::Tarantool16");

	types_boolean_stash = gv_stashpv("Types::Serialiser::Boolean", 1);

	types_true  = get_bool("Types::Serialiser::true");
	types_false = get_bool("Types::Serialiser::false");
}


void new(SV *pk, HV *conf)
	PPCODE:
		if (0) pk = pk;
		xs_ev_cnn_new(TntCnn); // declares YourType *self, set ST(0)
		self->default_on_connected_cb = self->cnn.on_connected;
		self->cnn.on_connected = (c_cb_conn_t) tnt_on_connected_cb;
		self->on_disconnect_before = (c_cb_discon_t) on_disconnect;
		self->cnn.on_read = (c_cb_read_t) on_greet_read;

		self->reqs = newHV();
		self->use_hash = 1;
		self->spaces = NULL;
		
		SV **key;
		if ((key = hv_fetchs(conf, "hash", 0)) ) self->use_hash = SvOK(*key) ? SvIV(*key) : 0;
		if ((key = hv_fetchs(conf, "username", 0)) && SvPOK(*key)) SvREFCNT_inc(self->username = *key);
		if ((key = hv_fetchs(conf, "password", 0)) && SvPOK(*key)) SvREFCNT_inc(self->password = *key);
		if ((key = hv_fetchs(conf, "log_level", 0)) && (SvOK(*key) && SvIOK(*key))) {
			self->log_level = SvIV(*key);
		} else {
			self->log_level = _LOG_INFO;
		}
		if ((key = hv_fetchs(conf, "wbuf_limit", 0))) {
			if (SvOK(*key)) {
				IV wbuf_limit = SvIV(*key);
				self->wbuf_limit = wbuf_limit > 0 ? wbuf_limit : 0;
			} else {
				self->wbuf_limit = TNT_WBUF_LIMIT;
			}
		}

		XSRETURN(1);


void DESTROY(SV *this)
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);

		if (!PL_dirty) {
			if (self->reqs) {
				free_reqs(self, "Destroyed");
				SvREFCNT_dec(self->reqs);
				self->reqs = NULL;
			}
			if (self->spaces) {
				destroy_spaces(self->spaces);
				self->spaces = NULL;
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
		xs_ev_cnn_checkconn_wlimit(self, cb, self->wbuf_limit);

		HV *opts = NULL;
		GET_OPTS(opts, items == 3 ? ST( 1 ) : 0, cb);
		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		uint32_t iid;
		INIT_CTX(self, ctx, "ping", iid);
		SV *pkt = pkt_ping(iid);
		EXEC_REQUEST_TIMEOUT(self, ctxsv, ctx, iid, pkt, opts, cb);

		XSRETURN_UNDEF;


void select( SV *this, SV *space, SV *keys, ... )
	PPCODE:
		if (0) this = this;
		// TODO: croak cleanup may be solved with refcnt+mortal
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn_wlimit(self, cb, self->wbuf_limit);

		HV *opts = NULL;
		GET_OPTS(opts, items == 5 ? ST( 3 ) : 0, cb);
		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		uint32_t iid;
		INIT_CTX(self, ctx, "select", iid);
		SV *pkt = pkt_select(ctx, iid, self->spaces, space, keys, opts, cb );
		EXEC_REQUEST_TIMEOUT(self, ctxsv, ctx, iid, pkt, opts, cb);

		XSRETURN_UNDEF;


void insert( SV *this, SV *space, SV *t, ... )
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn_wlimit(self, cb, self->wbuf_limit);

		HV *opts = NULL;
		GET_OPTS(opts, items == 5 ? ST( 3 ) : 0, cb);
		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		uint32_t iid;
		INIT_CTX(self, ctx, "insert", iid);
		SV *pkt = pkt_insert(ctx, iid, self->spaces, space, t, opts, cb );
		EXEC_REQUEST_TIMEOUT(self, ctxsv, ctx, iid, pkt, opts, cb);

		XSRETURN_UNDEF;
		
void replace( SV *this, SV *space, SV *t, ... )
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn_wlimit(self, cb, self->wbuf_limit);

		HV *opts = NULL;
		GET_OPTS(opts, items == 5 ? ST( 3 ) : 0, cb);
		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		uint32_t iid;
		INIT_CTX(self, ctx, "replace", iid);
		(void) hv_stores(opts, "replace", newSVuv(1));
		SV *pkt = pkt_insert(ctx, iid, self->spaces, space, t, opts, cb );
		EXEC_REQUEST_TIMEOUT(self, ctxsv, ctx, iid, pkt, opts, cb);

		XSRETURN_UNDEF;


void update( SV *this, SV *space, SV *key, SV *operations, ... )
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn_wlimit(self, cb, self->wbuf_limit);

		HV *opts = NULL;
		GET_OPTS(opts, items == 6 ? ST( 4 ) : 0, cb);
		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		uint32_t iid;
		INIT_CTX(self, ctx, "update", iid);
		SV *pkt = pkt_update(ctx, iid, self->spaces, space, key, operations, opts, cb );
		EXEC_REQUEST_TIMEOUT(self, ctxsv, ctx, iid, pkt, opts, cb);

		XSRETURN_UNDEF;


void upsert( SV *this, SV *space, SV *tuple, SV *operations, ... )
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn_wlimit(self, cb, self->wbuf_limit);

		HV *opts = NULL;
		GET_OPTS(opts, items == 6 ? ST( 4 ) : 0, cb);
		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		uint32_t iid;
		INIT_CTX(self, ctx, "upsert", iid);
		SV *pkt = pkt_upsert(ctx, iid, self->spaces, space, tuple, operations, opts, cb );
		EXEC_REQUEST_TIMEOUT(self, ctxsv, ctx, iid, pkt, opts, cb);

		XSRETURN_UNDEF;
		

void delete( SV *this, SV *space, SV *t, ... )
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn_wlimit(self, cb, self->wbuf_limit);

		HV *opts = NULL;
		GET_OPTS(opts, items == 5 ? ST( 3 ) : 0, cb);
		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		uint32_t iid;
		INIT_CTX(self, ctx, "delete", iid);
		SV *pkt = pkt_delete(ctx, iid, self->spaces, space, t, opts, cb );
		EXEC_REQUEST_TIMEOUT(self, ctxsv, ctx, iid, pkt, opts, cb);

		XSRETURN_UNDEF;


void eval( SV *this, SV *expression, SV *t, ... )
	PPCODE:
		if (0) this = this;
		// TODO: croak cleanup may be solved with refcnt+mortal
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn_wlimit(self, cb, self->wbuf_limit);

		HV *opts = NULL;
		GET_OPTS(opts, items == 5 ? ST( 3 ) : 0, cb);
		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		uint32_t iid;
		INIT_CTX(self, ctx, "eval", iid);
		SV *pkt = pkt_eval(ctx, iid, self->spaces, expression, t, opts, cb );
		EXEC_REQUEST_TIMEOUT(self, ctxsv, ctx, iid, pkt, opts, cb);

		XSRETURN_UNDEF;


void call( SV *this, SV *function_name, SV *t, ... )
	PPCODE:
		if (0) this = this;
		// TODO: croak cleanup may be solved with refcnt+mortal
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn_wlimit(self, cb, self->wbuf_limit);

		HV *opts = NULL;
		GET_OPTS(opts, items == 5 ? ST( 3 ) : 0, cb);
		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		uint32_t iid;
		INIT_CTX(self, ctx, "call", iid);
		SV *pkt = pkt_call(ctx, iid, self->spaces, function_name, t, opts, cb );
		EXEC_REQUEST_TIMEOUT(self, ctxsv, ctx, iid, pkt, opts, cb);

		XSRETURN_UNDEF;
