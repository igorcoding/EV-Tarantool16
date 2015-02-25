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

static void on_read(ev_cnn * self, size_t len) {
	cwarn("read %zu: %-.*s",len, (int)self->ruse, self->rbuf);
	cwarn("self->ruse: %zu", self->ruse);

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
	cwarn("pkt_length = %d", pkt_length);

	if (buf_len - 5 < pkt_length) {
		cwarn("not enough");
		return;
	}

	HV *hv = newHV();

	/* header */
	uint32_t id = 0;
	int length = parse_reply_hdr(hv, rbuf, buf_len, &id);
	cwarn("hdr_length = %d", length);
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

	length = parse_reply_body(hv, rbuf+length, buf_len, &ctx->f, ctx->use_hash ? ctx->space->fields : 0);
	cwarn("body length = %d", length);

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

	FREETMPS;
	LEAVE;
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

	cwarn("greeting success!");

	// perform authentication here

	self->ruse -= buf_len;
	cwarn("on_greet_read:: self->ruse: %d", (int)self->ruse);

	self->on_read = (c_cb_read_t) on_read;

	FREETMPS;
	LEAVE;
}

// static void on_read_(ev_cnn * self, size_t len) {
// 	debug("read %zu: %-.*s",len, (int)self->ruse, self->rbuf);
// 	//dSP;

// 	ENTER;
// 	SAVETMPS;
// 	//cwarn("remember stack sp = %d",PL_stack_sp - PL_stack_base);
// 	//SV **sp1 = PL_stack_sp;

// 	do_disable_rw_timer(self);
// 	//do_enable_rw_timer(self);
// 	TntCnn * tnt = (TntCnn *) self;
// 	char *rbuf = self->rbuf;
// 	char *end = rbuf + self->ruse;

// 	SV *key;
// 	TntCtx * ctx;

// 	dSP;

// 	while ( rbuf < end ) {
// 		tnt_hdr_t *hx = (tnt_hdr_t *) rbuf;
// 		uint32_t id  = le32toh( hx->reqid );
// 		uint32_t ln = le32toh( hx->len );
// 		//warn("reqid:%d; packet type: %d; len: %d",le32toh( hx->reqid ),le32toh( hx->type ),le32toh( hx->len ));
// 		if ( rbuf + 12 + ln <= end ) {
// 			debug("enough %p + 12 + %u < %p", rbuf,ln,end);

// 			key = hv_delete(tnt->reqs, (char *) &id, sizeof(id),0);

// 			if (!key) {
// 				cwarn("key %d not found",id);
// 				rbuf += 12 + ln;
// 			}
// 			else {
// 				ctx = ( TntCtx * ) SvPVX( key );
// 				ev_timer_stop(self->loop,&ctx->t);

// 				HV * hv = newHV();

// 				int length = parse_reply( hv, rbuf, ln+12, &ctx->f, ctx->use_hash ? ctx->space->fields : 0 );
// 				SV ** var = hv_fetchs(hv,"code",0);
// 				if (var && SvIV (*var) != 0) {


// 				//	warn("reqid:%d; %s\n\n",id, dumper(ctx->wbuf));
// 				}

// 				SvREFCNT_dec(ctx->wbuf);
// 				if (ctx->f.size && !ctx->f.nofree) {
// 					safefree(ctx->f.f);
// 				}
// 				if (length > 0) {
// 					(void) hv_stores(hv, "size", newSVuv(length));
// 				}

// 				if (ctx->cb) {
// 					//cwarn("read sp in  = %p (%d)",sp, PL_stack_sp - PL_stack_base);

// 					SPAGAIN;

// 					ENTER; SAVETMPS;

// 					SV ** var = hv_fetchs(hv,"code",0);
// 					if (var && SvIV (*var) == 0) {
// 						PUSHMARK(SP);
// 						EXTEND(SP, 1);
// 						PUSHs( sv_2mortal(newRV_noinc( (SV *) hv )) );
// 						PUTBACK;
// 					}
// 					else {
// 						var = hv_fetchs(hv,"errstr",0);
// 						PUSHMARK(SP);
// 						EXTEND(SP, 3);
// 						PUSHs( &PL_sv_undef );
// 						PUSHs( var && *var ? sv_2mortal(newSVsv(*var)) : &PL_sv_undef );
// 						PUSHs( sv_2mortal(newRV_noinc( (SV *) hv )) );
// 						PUTBACK;
// 					}

// 					(void) call_sv( ctx->cb, G_DISCARD | G_VOID );

// 					//SPAGAIN;PUTBACK;

// 					SvREFCNT_dec(ctx->cb);

// 					FREETMPS; LEAVE;
// 				}

// 				--tnt->pending;

// 				rbuf += 12 + ln;
// 				if (rbuf == end) {
// 					self->ruse = 0;
// 					if (tnt->pending == 0) {
// 						//do_disable_rw_timer(self);
// 					}
// 					else {
// 						//do_enable_rw_timer(self);
// 					}
// 					break;
// 				}
// 			}
// 		}
// 		else {
// 			debug("need more");
// 			break;
// 		}
// 	}
// 	self->ruse = end - rbuf;
// 	if (self->ruse > 0) {
// 		//cwarn("move buf on %zu",self->ruse);
// 		memmove(self->rbuf,rbuf,self->ruse);
// 	}

// 	FREETMPS;
// 	LEAVE;
// }

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

MODULE = EV::Tarantool		PACKAGE = EV::Tarantool
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

		if ((key = hv_fetchs(conf, "spaces", 0)) && SvROK(*key)) {
			configure_spaces( self->spaces, *key );
		}
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
		cwarn("wbuf_size: %zu", SvCUR(ctx->wbuf));

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
			cwarn("wbuf_size: %zu", SvCUR(ctx->wbuf));

			SvREFCNT_inc(ctx->cb = cb);
			(void) hv_store( self->reqs, (char*)&iid, sizeof(iid), SvREFCNT_inc(ctxsv), 0 );

			++self->pending;

			do_write( &self->cnn,SvPVX(ctx->wbuf), SvCUR(ctx->wbuf));
		}

		XSRETURN_UNDEF;
