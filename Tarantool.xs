#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#include "EVAPI.h"

//#define MYDEBUG

#include "ev_sock.c"
#include "tnt.c"

#define dSELF(type, from) register type *self = ( type * ) SvUV( SvRV( from ) )
#define dSVX(sv,ref,type) \
	SV *sv = newSV( sizeof(type) );\
	SvUPGRADE( sv, SVt_PV ); \
	SvCUR_set(sv,sizeof(type)); \
	SvPOKp_on(sv); \
	type * ref = (type *) SvPVX( sv );


#define dSVPV(sv,size) \
	SV *sv = newSV( size );\
	SvUPGRADE( sv, SVt_PV ); \
	SvCUR_set(sv,size); \
	SvPOKp_on(sv);


static void call_back_cv( SV * cv, int args, ...);

static inline int sv_inet_aton(SV * sv_addr) {
	struct in_addr ip;
	inet_aton(SvPV_nolen(sv_addr), &ip);
	return ip.s_addr;
}

typedef struct {
	Cnn cnn;
	struct iovec iov;
	SV *self;
	SV *wbuf;
	SV *rbuf;
	uint32_t pending;
	uint32_t seq;
	HV *reqs;
	SV *connected;
} TntCnn;

typedef struct {
	SV * cb;
	SV * wbuf;
	unpack_format f;
} TntCtx;

void on_disconnect(Cnn *self, int err) {
	
	debug("disconnect: %s",strerror(err));
}
void on_connfail(Cnn *self, int err) {
	Info  * info = (Info *) self->any;
	debug("connfail: %s [%d]",strerror(err), ++info->reconnect_count);
	if (info->reconnect_count > 5) {
		//do_disconnect(self);
	}
}

void on_connected(Cnn *self, struct sockaddr_in *peer) {
	debug("connected address: %s:%d", inet_ntoa( peer->sin_addr ), ntohs( peer->sin_port ) );
	TntCnn * tnt = (TntCnn *) self;
	call_back_cv( tnt->connected, 3, tnt->self, sv_2mortal( newSVpv( inet_ntoa( peer->sin_addr ),0 ) ), sv_2mortal( newSVuv( ntohs( peer->sin_port ) ) )  );
	//do_write(self, "GET test\n",0);
}

void on_read(Cnn * self, size_t len) {
	debug("read %zu: %-.*s",len, self->ruse, self->rbuf);
	dSP;
	ENTER;
	SAVETMPS;
	
	do_disable_rw_timer(self);
	//do_enable_rw_timer(self);
	TntCnn * tnt = (TntCnn *) self;
	char *rbuf = self->rbuf;
	char *end = rbuf + self->ruse;
	
	SV *key;
	TntCtx * ctx;
	while ( rbuf < end ) {
		tnt_hdr_t *hx = (tnt_hdr_t *) rbuf;
		uint32_t id  = le32toh( hx->reqid );
		uint32_t ln = le32toh( hx->len );
		debug("packet type: %04x; id:%04x; len: %04x",le32toh( hx->type ),le32toh( hx->reqid ),le32toh( hx->len ));
		if ( rbuf + 12 + ln <= end ) {
			debug("enough %p + 12 + %u < %p", rbuf,ln,end);
			key = hv_delete(tnt->reqs, (char *) &id, sizeof(id),0);
			if (!key) {
				cwarn("key %d not found",id);
				rbuf += 12 + ln;
			}
			else {
				ctx = ( TntCtx * ) SvPVX( key );
				SvREFCNT_dec(ctx->wbuf);
				
				HV * hv = newHV();
				
				int length = parse_reply( hv, rbuf, len+12, &ctx->f );
				if (ctx->f.size && ctx->f.f) {
					safefree(ctx->f.f);
				}
				//if (length > 0) {
				//	(void) hv_stores(hv, "size", newSVuv(length));
				//}
				
				SV * res = sv_2mortal(newRV_noinc( (SV *) hv ));
				
				if (ctx->cb) {
					ENTER;
					SAVETMPS;
					PUSHMARK(SP);
					EXTEND(SP, 1);
				
					XPUSHs( res );
					
					PUTBACK;
					
					call_sv( ctx->cb, G_DISCARD | G_VOID );
					//call_sv( cv, G_DISCARD | G_VOID | G_EVAL | G_KEEPERR );
					
					SvREFCNT_dec(ctx->cb);
				
					FREETMPS;
					LEAVE;
				}
			
				--tnt->pending;
				
				rbuf += 12 + ln;
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
		else {
			debug("need more");
			break;
		}
	}
	self->ruse = end - rbuf;
	if (self->ruse > 0) memmove(self->rbuf,rbuf,self->ruse);
	
	FREETMPS;
	LEAVE;
}

static void call_back_cv( SV * cv, int args, ...) {
	if (!cv) {
		cwarn("no handler");
		return;
	}
	dSP;
	int i;
	va_list ap;
	va_start(ap, args);
	
	ENTER;
	SAVETMPS;
	
	PUSHMARK(SP);
	EXTEND( SP, args );
	
	for (i=0; i < args; i++) {
		SV * arg = va_arg(ap, SV *);
		if (arg && SvOK( arg )) {
			//debug("push arg: %p (%d) %s", arg, arg ? SvTYPE(arg) : -1, arg ? SvPV_nolen(arg) : "NULL");
			XPUSHs( arg );
		}
		else {
			//debug("skip arg: %p (%d) %s", arg, arg ? SvTYPE(arg) : -1, arg ? SvPV_nolen(arg) : "NULL");
			XPUSHs( UNDEF );
		}
	}
	va_end(ap);
	PUTBACK;
	//debug("call %p (%s)", cv, SvPV_nolen(cv));
	call_sv( cv, G_DISCARD | G_VOID );
	
	//call_sv( cv, G_DISCARD | G_VOID | G_EVAL | G_KEEPERR );
	
	//SvREFCNT_dec( r->cb );
	//r->cb = 0;
	
	//if
	FREETMPS;
	LEAVE;
	
	//end
	
}

HV *DESstash;

MODULE = EV::Tarantool		PACKAGE = EV::Tarantool::DES

void DESTROY(SV *this)
	PPCODE:
		debug("DESTROY %p -> %p (%d)",this,SvRV(this),SvREFCNT( SvRV(this) ));


MODULE = EV::Tarantool		PACKAGE = EV::Tarantool

BOOT:
{
	I_EV_API ("EV::Tarantool");
	DESstash = gv_stashpv("EV::Tarantool::DES", TRUE);
}

void new(SV *pk, HV *conf)
	PPCODE:
		HV *stash = gv_stashpv(SvPV_nolen(pk), TRUE);
		TntCnn * self = safemalloc( sizeof(TntCnn) );
		memset(self,0,sizeof(TntCnn));
		Cnn *cnn = &self->cnn;
		SV **key;
		//self->stash = stash;
		self->self = sv_bless (newRV_noinc (newSViv(PTR2IV( self ))), stash);
		ST(0) = self->self;
		//ST(0) = sv_2mortal (sv_bless (newRV_noinc (newSViv(PTR2IV( self ))), stash));
		//debug("new: self.self = %p / %s (%d)",self->self, SvPV_nolen(self->self), SvREFCNT(self->self));
		
		debug("default loop = %p",EV_DEFAULT);
		
		cnn->loop = EV_DEFAULT;
		cnn->connect_timeout = 1.0;
		cnn->rw_timeout = 1.0;
		
		self->reqs = newHV();
		self->rbuf = newSV( 4096*10 );
		SvUPGRADE( self->rbuf, SVt_PV );
		
		//self->iov.iov_len = SvLEN( self->wbuf );
		//self->iov.iov_base = SvPVX( self->wbuf );
		cnn->rbuf = SvPVX(self->rbuf);
		cnn->rlen = SvLEN(self->rbuf);
		
		cnn->iov = &self->iov;
		cnn->iovcnt = 1;
		if ((key = hv_fetch(conf, "connected", 9, 0)) && SvROK(*key)) {
			self->connected = *key;
			SvREFCNT_inc(self->connected);
		}
		/*
		if ((key = hv_fetch(conf, "onread", 6, 0)) && SvROK(*key)) {
			self->onread = *key;
			SvREFCNT_inc(self->onread);
		}
		*/
		if ((key = hv_fetch(conf, "host", 4, 0)) && SvOK(*key)) {
			self->cnn.iaddr.sin_addr.s_addr = sv_inet_aton( *key );
		}
		else { croak("host required"); }
		if ((key = hv_fetch(conf, "port", 4, 0)) && SvOK(*key)) {
			self->cnn.iaddr.sin_port = htons( SvUV( *key ) );
		}
		else { croak("port required"); }
		self->cnn.iaddr.sin_family      = AF_INET;
		do_check(cnn);

		XSRETURN(1);


void DESTROY(SV *this)
	PPCODE:
		dSELF(TntCnn, this);
		debug("destroy");
		//SV * leak = newSV(1024);
		do_destroy(&self->cnn);
		if (self->rbuf) SvREFCNT_dec(self->rbuf);
		if (self->wbuf) SvREFCNT_dec(self->wbuf);
		if (self->reqs) SvREFCNT_dec(self->reqs);
		/*
		if (self->onread) {
			SvREFCNT_dec(self->onread);
		}
		if (self->disconnected) {
			SvREFCNT_dec(self->disconnected);
		}
		if (self->connfail) {
			SvREFCNT_dec(self->connfail);
		}
		if (self->wbuf) {
			SvREFCNT_dec(self->wbuf);
		}
		*/
		safefree(self);

void connect(SV *this)
	PPCODE:
		dSELF(TntCnn, this);
		debug("%p, %p",this, self);
		do_connect(&self->cnn);
		XSRETURN_UNDEF;

void reqs(SV *this)
	PPCODE:
		dSELF(TntCnn, this);
		ST(0) = sv_2mortal(newRV_inc((SV *)self->reqs));
		XSRETURN(1);

void ping(SV *this, SV * cb)
	PPCODE:
		dSELF(TntCnn, this);
		dSVX(ctxsv, ctx, TntCtx);
		ctx->f.size = 0;
		
		uint32_t iid = ++self->seq;
		
		SvREFCNT_inc(ctx->cb = cb);
		
		(void) hv_store( self->reqs, (char*)&iid, sizeof(iid), ctxsv, 0 );
		
		debug("send ping with seq id %u",iid);
		
		ctx->wbuf   = newSV(12);
		SvUPGRADE( ctx->wbuf, SVt_PV );
		
		tnt_hdr_t *s = (tnt_hdr_t *) SvPVX( ctx->wbuf );
		s->type  = htole32( TNT_OP_PING );
		s->reqid = htole32( iid );
		s->len   = 0;
		
		++self->pending;
		
		do_write( &self->cnn,SvPVX(ctx->wbuf),12 );
		
		XSRETURN_UNDEF;

void lua( SV *this, SV * proc, AV * tuple, ... )
	PPCODE:
		register uniptr p;
		dSELF(TntCnn, this);
		dUnpackFormat( format );
		SV *cb;
		HV *opt = 0;
		if (items == 5) {
			opt = (HV *) SvRV(ST( 3 ));
			cb = ST(4);
		}
		else {
			cb = ST(3);
		}
		debug("opt = %p, c = %s",opt, SvPV_nolen(cb));
		
		dSVX(ctxsv, ctx, TntCtx);
		uint32_t iid = ++self->seq;
		SvREFCNT_inc(ctx->cb = cb);
		(void) hv_store( self->reqs, (char*)&iid, sizeof(iid), ctxsv, 0 );
		debug("send lua with seq id %u",iid);
		int flags = 0;
		if (opt) {
			SV **key;
			key = hv_fetch(opt,"out",3,0);
			if (key && *key) {
				//warn("use out format %s",SvPV_nolen(*key));
				dExtractFormat2( format, *key );
				if (format.size) {
					ctx->f.size = format.size;
					ctx->f.f   = safemalloc(format.size);
					memcpy(ctx->f.f,format.f,format.size);
					ctx->f.def = format.def;
				}
				else {
					ctx->f.size = 0;
				}
			}
			key = hv_fetch(opt,"in",2,0);
			if (key && *key) {
				//warn("use in format %s",SvPV_nolen(*key));
				dExtractFormat2( format, *key );
			}
		}
		else {
			ctx->f.f = 0;
			ctx->f.size = 0;
			ctx->f.def = 'p';
		}
		
		//dExtractFormat( format, 4, "lua( req_id, flags, proc, tuple" );
		int k;
		
		ctx->wbuf = newSVpvn("",0);
		
		tnt_pkt_call_t *h = (tnt_pkt_call_t *)
			SvGROW( ctx->wbuf, 
				( ( (
					sizeof( tnt_pkt_call_t ) +
					+ 4
					+ sv_len(proc)
					+ ( av_len(tuple)+1 ) * ( 5 + 32 )
					+ 16
				) >> 5 ) << 5 ) + 0x20
			);
		
		p.c = (char *)(h+1);
		
		uptr_field_sv_fmt( p, proc, 'p' );
		
		*(p.i++) = htole32( av_len(tuple) + 1 );
		
		for (k=0; k <= av_len(tuple); k++) {
			SV *f = *av_fetch( tuple, k, 0 );
			if ( !SvOK(f) || !sv_len(f) ) {
				*(p.c++) = 0;
			} else {
				uptr_sv_size( p, ctx->wbuf, 5 + sv_len(f) );
				uptr_field_sv_fmt( p, f, k < format.size ? format.f[k] : format.def );
			}
		}
		
		SvCUR_set( ctx->wbuf, p.c - SvPVX(ctx->wbuf) );
		
		h = (tnt_pkt_call_t *) SvPVX( ctx->wbuf ); // for sure
		h->type   = htole32( TNT_OP_CALL );
		h->reqid  = htole32( iid );
		h->flags  = htole32( flags );
		h->len    = htole32( SvCUR(ctx->wbuf) - sizeof( tnt_hdr_t ) );
		
		++self->pending;
		
		do_write( &self->cnn,SvPVX(ctx->wbuf), SvCUR(ctx->wbuf));
		
		XSRETURN_UNDEF;

	//item select( $space, $keys, [ $opts = { index = 0, limit = 2**32, offset = 0 }, ] $cb->( $result, $error ) )

void select( SV *this, U32 ns, AV * keys, ... )
	PPCODE:
		register uniptr p;
		U32 limit = 0xffffffff;
		U32 offset = 0;
		U32 index = 0;
		int k,i;
		dSELF(TntCnn, this);
		dUnpackFormat( format );
		uint32_t iid = ++self->seq;
		SV *cb;
		HV *opt = 0;
		dSVX(ctxsv, ctx, TntCtx);
		if (items == 5) {
			opt = (HV *) SvRV(ST( 3 ));
			SV **key;
			if ((key = hv_fetch(opt, "index", 5, 0)) && SvOK(*key)) index = SvUV(*key);
			if ((key = hv_fetch(opt, "limit", 5, 0)) && SvOK(*key)) limit = SvUV(*key);
			if ((key = hv_fetch(opt, "offset", 6, 0)) && SvOK(*key)) offset = SvUV(*key);
			
			// TODO: space config
			
			if ((key = hv_fetch(opt,"out",3,0)) && *key) {
				dExtractFormat2( format, *key );
				if (format.size) {
					ctx->f.size = format.size;
					ctx->f.f    = safemalloc(format.size);
					memcpy(ctx->f.f,format.f,format.size);
					ctx->f.def  = format.def;
					cwarn("using out format: %s",format.f);
				}
				else {
					ctx->f.size = 0;
				}
			}
			
			if ((key = hv_fetch(opt,"in",2,0)) && *key) {
				dExtractFormat2( format, *key );
				cwarn("using in format: %s",format.f);
			}
			
			cb = ST(4);
		}
		else {
			cb = ST(3);
			ctx->f.size = 0;
		}
		debug("opt = %p, c = %s",opt, SvPV_nolen(cb));
		
		SV *sv = newSVpvn("",0);
		
		tnt_pkt_select_t *h = (tnt_pkt_select_t *)
			SvGROW( sv, 
				( ( (
					sizeof( tnt_pkt_select_t ) +
					+ 4
					+ ( av_len(keys)+1 ) * ( 5 + 32 )
					+ 16
				) >> 5 ) << 5 ) + 0x20
			);
		
		p.c = (char *)(h+1);
		
		for (i = 0; i <= av_len(keys); i++) {
			SV *t = *av_fetch( keys, i, 0 );
			if (!SvROK(t) || (SvTYPE(SvRV(t)) != SVt_PVAV)) {
				sv_2mortal(sv);
				sv_2mortal(ctxsv);
				croak("keys must be ARRAYREF of ARRAYREF");
			}
			AV *fields = (AV *) SvRV(t);
			
			*( p.i++ ) = htole32( av_len(fields) + 1 );
			
			for (k=0; k <= av_len(fields); k++) {
				SV *f = *av_fetch( fields, k, 0 );
				if ( !SvOK(f) || !sv_len(f) ) {
					*(p.c++) = 0;
				} else {
					uptr_sv_size( p, sv, 5 + sv_len(f) );
					uptr_field_sv_fmt( p, f, k < format.size ? format.f[k] : format.def );
				}
			}
		}
		
		SvCUR_set( sv, p.c - SvPVX(sv) );
		
		h = (tnt_pkt_select_t *) SvPVX( sv ); // for sure
		
		h->type   = htole32( TNT_OP_SELECT );
		h->reqid  = htole32( htole32( iid ) );
		h->space  = htole32( htole32( ns ) );
		h->index  = htole32( htole32( index ) );
		h->offset = htole32( htole32( offset ) );
		h->limit  = htole32( htole32( limit ) );
		h->count  = htole32( htole32( av_len(keys) + 1 ) );
		h->len    = htole32( SvCUR(sv) - sizeof( tnt_hdr_t ) );

		SvREFCNT_inc(ctx->cb = cb);
		ctx->wbuf = sv;
		(void) hv_store( self->reqs, (char*)&iid, sizeof(iid), ctxsv, 0 );
		
		++self->pending;
		
		do_write( &self->cnn,SvPVX(ctx->wbuf), SvCUR(ctx->wbuf));
		
		XSRETURN_UNDEF;
		//ST(0) = sv;
		//XSRETURN(1);
