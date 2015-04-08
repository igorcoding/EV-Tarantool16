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

typedef struct {
	xs_ev_cnn_struct;

	void (*on_disconnect_before)(void *, int);
	void (*on_disconnect_after)(void *, int);
	void (*on_connect_before)(void *, struct sockaddr *);
	void (*on_connect_after)(void *, struct sockaddr *);

	uint32_t pending;
	uint32_t seq;
	U32      use_hash;
	HV      *reqs;
	HV      *spaces;
	SV      *server_version;
	SV      *salt;
} TntCnn;

typedef void (*pre_full_connect_cb_t)(TntCnn *cnn, HV *data);

static const uint32_t _SPACE_SPACEID = 280;
static const uint32_t _INDEX_SPACEID = 288;

static void on_request_timer(EV_P_ ev_timer *t, int flags) {
	TntCtx * ctx = (TntCtx *) t;
	TntCnn * self = (TntCnn *) ctx->self;
	cwarn("timer called on %p: %s", ctx, ctx->call);
	ENTER;SAVETMPS;
	dSP;

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

	hv_delete( self->reqs, (char *) &ctx->id, sizeof(ctx->id),0);

	--self->pending;

	FREETMPS;LEAVE;
}

static void _execute_select(TntCnn * tnt, uint32_t space_id) {
	dSVX(ctxsv, ctx, TntCtx);
	sv_2mortal(ctxsv);
	ctx->call = "select";
	ctx->use_hash = tnt->use_hash;

	uint32_t iid = ++tnt->seq;

	if ((ctx->wbuf = pkt_select(ctx, iid, NULL, sv_2mortal(newSVuv(space_id)), newRV_noinc((SV *) newAV()), NULL, NULL ))) {
		// cwarn("wbuf_size: %zu", SvCUR(ctx->wbuf));

		SvREFCNT_inc(ctx->cb = NULL);
		(void) hv_store( tnt->reqs, (char*)&iid, sizeof(iid), SvREFCNT_inc(ctxsv), 0 );

		++tnt->pending;

		do_write( &tnt->cnn,SvPVX(ctx->wbuf), SvCUR(ctx->wbuf));
	}
}

static void on_read(ev_cnn * self, size_t len) {
	// cwarn("read %zu: %-.*s",len, (int)self->ruse, self->rbuf);
	// cwarn("self->ruse: %zu", self->ruse);

	ENTER;
	SAVETMPS;

	do_disable_rw_timer(self);

	TntCnn * tnt = (TntCnn *) self;
	char *rbuf = self->rbuf;
	char *end = rbuf + self->ruse;

	/* len */
	ptrdiff_t buf_len = end - rbuf;
	if (buf_len == 0) {
		cwarn("buflen==0. weird.");
		return;
	}

	uint32_t pkt_length = decode_pkt_len(&rbuf);
	// cwarn("pkt_length = %d", pkt_length);

	if (buf_len - 5 < pkt_length) {
		cwarn("not enough");
		return;
	}

	HV *hv = newHV();

	/* header */
	uint32_t id = 0;
	int length = parse_reply_hdr(hv, rbuf, buf_len, &id);
	// cwarn("hdr_length = %d", length);
	if (unlikely(id == 0)) {
		// TODO: error
		cwarn("id == 0");
		return;
	}

	TntCtx *ctx;
	SV *key = hv_delete(tnt->reqs, (char *) &id, sizeof(id), 0);

	if (!key) {
		cwarn("key %d not found", id);
		return;
	}
	else {
		ctx = (TntCtx *) SvPVX(key);
		ev_timer_stop(self->loop, &ctx->t);
	}

	/* body */
	rbuf += length;

	AV *fields = (ctx->space && ctx->use_hash) ? ctx->space->fields : NULL;
	length = parse_reply_body(hv, rbuf, buf_len, &ctx->f, fields);
	// cwarn("body length = %d", length);
	rbuf += length;

	dSP;

	if (ctx->cb) {
		SPAGAIN;

		ENTER; SAVETMPS;

		SV ** var = hv_fetchs(hv,"code",0);
		if (var && SvIV (*var) == 0) {
			PUSHMARK(SP);
			EXTEND(SP, 1);
			PUSHs( sv_2mortal(newRV_noinc( (SV *) hv )) );
			PUTBACK;
		}
		else {
			var = hv_fetchs(hv,"errstr",0);
			PUSHMARK(SP);
			EXTEND(SP, 3);
			PUSHs( &PL_sv_undef );
			PUSHs( var && *var ? sv_2mortal(newSVsv(*var)) : &PL_sv_undef );
			PUSHs( sv_2mortal(newRV_noinc( (SV *) hv )) );
			PUTBACK;
		}

		(void) call_sv(ctx->cb, G_DISCARD | G_VOID);

		//SPAGAIN;PUTBACK;

		SvREFCNT_dec(ctx->cb);

		FREETMPS; LEAVE;
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

static void on_index_info_read(ev_cnn * self, size_t len) {
	// cwarn("read %zu: %-.*s",len, (int)self->ruse, self->rbuf);
	// cwarn("self->ruse: %zu", self->ruse);

	// ENTER;
	// SAVETMPS;

	do_disable_rw_timer(self);

	TntCnn * tnt = (TntCnn *) self;
	char *rbuf = self->rbuf;
	char *end = rbuf + self->ruse;

	/* len */
	ptrdiff_t buf_len = end - rbuf;
	if (buf_len == 0) {
		cwarn("buflen==0. weird.");
		return;
	}

	uint32_t pkt_length = decode_pkt_len(&rbuf);
	// cwarn("pkt_length = %d", pkt_length);

	if (buf_len - 5 < pkt_length) {
		cwarn("not enough");
		return;
	}

	HV *spaces_hv = newHV();

	/* header */
	uint32_t id = 0;
	int length = parse_reply_hdr(spaces_hv, rbuf, buf_len, &id);
	// cwarn("hdr_length = %d", length);
	if (unlikely(id == 0)) {
		// TODO: error
		cwarn("id == 0");
		return;
	}

	TntCtx *ctx;
	SV *key = hv_delete(tnt->reqs, (char *) &id, sizeof(id), 0);

	if (!key) {
		cwarn("key %d not found", id);
		return;
	}
	else {
		ctx = (TntCtx *) SvPVX(key);
		ev_timer_stop(self->loop, &ctx->t);
	}
	rbuf += length;

	/* body */

	// check that status is ok

	length = parse_index_body(tnt->spaces, rbuf, buf_len);
	// cwarn("body length = %d", length);
	rbuf += length;

	--tnt->pending;

	self->ruse = end - rbuf;
	if (self->ruse > 0) {
		//cwarn("move buf on %zu",self->ruse);
		memmove(self->rbuf,rbuf,self->ruse);
	}

	// tnt->spaces = newRV_noinc(spaces_hv);
	self->on_read = (c_cb_read_t) on_read;

	// FREETMPS;
	// LEAVE;
}

static void on_spaces_info_read(ev_cnn * self, size_t len) {
	// cwarn("read %zu: %-.*s",len, (int)self->ruse, self->rbuf);
	// cwarn("self->ruse: %zu", self->ruse);

	// ENTER;
	// SAVETMPS;

	do_disable_rw_timer(self);

	TntCnn * tnt = (TntCnn *) self;
	char *rbuf = self->rbuf;
	char *end = rbuf + self->ruse;

	/* len */
	ptrdiff_t buf_len = end - rbuf;
	if (buf_len == 0) {
		cwarn("buflen==0. weird.");
		return;
	}

	uint32_t pkt_length = decode_pkt_len(&rbuf);
	// cwarn("pkt_length = %d", pkt_length);

	if (buf_len - 5 < pkt_length) {
		cwarn("not enough");
		return;
	}

	HV *spaces_hv = newHV();

	/* header */
	uint32_t id = 0;
	int length = parse_reply_hdr(spaces_hv, rbuf, buf_len, &id);
	// cwarn("hdr_length = %d", length);
	if (unlikely(id == 0)) {
		// TODO: error
		cwarn("id == 0");
		return;
	}

	TntCtx *ctx;
	SV *key = hv_delete(tnt->reqs, (char *) &id, sizeof(id), 0);

	if (!key) {
		cwarn("key %d not found", id);
		return;
	}
	else {
		ctx = (TntCtx *) SvPVX(key);
		ev_timer_stop(self->loop, &ctx->t);
	}
	rbuf += length;

	/* body */

	length = parse_spaces_body(spaces_hv, rbuf, buf_len);
	// cwarn("body length = %d", length);
	rbuf += length;

	--tnt->pending;

	self->ruse = end - rbuf;
	if (self->ruse > 0) {
		//cwarn("move buf on %zu",self->ruse);
		memmove(self->rbuf,rbuf,self->ruse);
	}


	SV **spaces_hv_key;
	if ((spaces_hv_key = hv_fetchs( spaces_hv, "data", 0)) && SvOK(*spaces_hv_key)) {
		tnt->spaces = SvRV(*spaces_hv_key);
	}
	self->on_read = (c_cb_read_t) on_index_info_read;
	_execute_select(tnt, _INDEX_SPACEID);
	// self->on_read = (c_cb_read_t) on_read;

	// FREETMPS;
	// LEAVE;
}


static void on_greet_read(ev_cnn * self, size_t len) {
	cwarn("greet_read %zu: %-.*s",len, (int)self->ruse, self->rbuf);

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

	//TODO: perform authentication here and save salt and server version

	self->ruse -= buf_len;
	cwarn("on_greet_read:: self->ruse: %d", (int)self->ruse);

	self->on_read = (c_cb_read_t) on_spaces_info_read;
	// tnt->pre_full_connect_cb = (pre_full_connect_cb_t) &pre_connect_on_spaces_read;
	//TODO: perform _spaces select
	_execute_select(tnt, _SPACE_SPACEID);



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
			PUSHs( sv_2mortal(newSVpvf(message)) );
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

INLINE SV *get_bool (const char *name) {
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
		self->cnn.on_read = (c_cb_read_t) on_greet_read;
		// self->cnn.on_read = (c_cb_read_t) on_read;
		self->on_disconnect_before = on_disconnect;


		//cwarn("new     this: %p; iv[%d]: %p; self: %p; self->self: %p",ST(0), SvREFCNT(iv),iv, self, self->self);

		SV **key;

		self->reqs = newHV();

		self->use_hash = 1;
		if ((key = hv_fetchs(conf, "hash", 0)) ) self->use_hash = SvOK(*key) ? SvIV(*key) : 0;

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


void ping(SV *this, SV * cb)
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);
		xs_ev_cnn_checkconn(self,cb);

		dSVX(ctxsv, ctx, TntCtx);
		ctx->call = "ping";

		uint32_t iid = ++self->seq;
		SvREFCNT_inc(ctx->cb = cb);
		(void) hv_store( self->reqs, (char*)&iid, sizeof(iid), ctxsv, 0 );

		ctx->wbuf = pkt_ping(iid);
		// cwarn("wbuf_size: %zu", SvCUR(ctx->wbuf));

		++self->pending;
		do_write( &self->cnn,SvPVX(ctx->wbuf), SvCUR(ctx->wbuf) );

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
		ctx->call = "select";
		ctx->use_hash = self->use_hash;

		uint32_t iid = ++self->seq;

		if ((ctx->wbuf = pkt_select(ctx, iid, self->spaces, space, keys, items == 5 ? (HV *) SvRV(ST( 3 )) : 0, cb ))) {
			// cwarn("wbuf_size: %zu", SvCUR(ctx->wbuf));

			SvREFCNT_inc(ctx->cb = cb);
			(void) hv_store( self->reqs, (char*)&iid, sizeof(iid), SvREFCNT_inc(ctxsv), 0 );

			++self->pending;

			do_write( &self->cnn,SvPVX(ctx->wbuf), SvCUR(ctx->wbuf));
		}

		XSRETURN_UNDEF;


void insert( SV *this, SV *space, SV * t, ... )
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn(self,cb);

		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		ctx->call = "insert";
		ctx->use_hash = self->use_hash;

		uint32_t iid = ++self->seq;

		if(( ctx->wbuf = pkt_insert(ctx, iid, self->spaces, space, t, items == 5 ? (HV *) SvRV(ST( 3 )) : 0, cb ) )) {

			SvREFCNT_inc(ctx->cb = cb);
			(void) hv_store( self->reqs, (char*)&iid, sizeof(iid), SvREFCNT_inc(ctxsv), 0 );
			++self->pending;
			do_write( &self->cnn,SvPVX(ctx->wbuf), SvCUR(ctx->wbuf));
		}

		XSRETURN_UNDEF;


void update( SV *this, SV *space, SV * key, SV * tuple, ... )
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn(self,cb);

		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		ctx->call = "insert";
		ctx->use_hash = self->use_hash;

		uint32_t iid = ++self->seq;

		if(( ctx->wbuf = pkt_update(ctx, iid, self->spaces, space, key, tuple, items == 6 ? (HV *) SvRV(ST( 4)) : 0, cb ) )) {

			SvREFCNT_inc(ctx->cb = cb);
			(void) hv_store( self->reqs, (char*)&iid, sizeof(iid), SvREFCNT_inc(ctxsv), 0 );
			++self->pending;
			do_write( &self->cnn,SvPVX(ctx->wbuf), SvCUR(ctx->wbuf));
		}

		XSRETURN_UNDEF;


void delete( SV *this, SV *space, SV * t, ... )
	PPCODE:
		if (0) this = this;
		xs_ev_cnn_self(TntCnn);
		SV *cb = ST(items-1);
		xs_ev_cnn_checkconn(self,cb);

		dSVX(ctxsv, ctx, TntCtx);
		sv_2mortal(ctxsv);
		ctx->call = "delete";
		ctx->use_hash = self->use_hash;

		uint32_t iid = ++self->seq;

		if(( ctx->wbuf = pkt_delete(ctx, iid, self->spaces, space, t, items == 5 ? (HV *) SvRV(ST( 3 )) : 0, cb ) )) {

			SvREFCNT_inc(ctx->cb = cb);
			(void) hv_store( self->reqs, (char*)&iid, sizeof(iid), SvREFCNT_inc(ctxsv), 0 );
			++self->pending;
			do_write( &self->cnn,SvPVX(ctx->wbuf), SvCUR(ctx->wbuf));
		}

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
		ctx->call = "eval";
		ctx->use_hash = self->use_hash;

		uint32_t iid = ++self->seq;

		if ((ctx->wbuf = pkt_eval(ctx, iid, self->spaces, expression, t, items == 5 ? (HV *) SvRV(ST( 3 )) : 0, cb ))) {
			// cwarn("wbuf_size: %zu", SvCUR(ctx->wbuf));

			SvREFCNT_inc(ctx->cb = cb);
			(void) hv_store( self->reqs, (char*)&iid, sizeof(iid), SvREFCNT_inc(ctxsv), 0 );

			++self->pending;

			do_write( &self->cnn,SvPVX(ctx->wbuf), SvCUR(ctx->wbuf));
		}

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
		ctx->call = "eval";
		ctx->use_hash = self->use_hash;

		uint32_t iid = ++self->seq;

		if ((ctx->wbuf = pkt_call(ctx, iid, self->spaces, function_name, t, items == 5 ? (HV *) SvRV(ST( 3 )) : 0, cb ))) {
			// cwarn("wbuf_size: %zu", SvCUR(ctx->wbuf));

			SvREFCNT_inc(ctx->cb = cb);
			(void) hv_store( self->reqs, (char*)&iid, sizeof(iid), SvREFCNT_inc(ctxsv), 0 );

			++self->pending;

			do_write( &self->cnn,SvPVX(ctx->wbuf), SvCUR(ctx->wbuf));
		}

		XSRETURN_UNDEF;
