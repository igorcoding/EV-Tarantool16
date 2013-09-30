#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#include "EVAPI.h"
#include "ext/EVSock.h"
//#define MYDEBUG

#include "tnt.h"

#define dSVX(sv,ref,type) \
	SV *sv = newSV( sizeof(type) );\
	SvUPGRADE( sv, SVt_PV ); \
	SvCUR_set(sv,sizeof(type)); \
	SvPOKp_on(sv); \
	type * ref = (type *) SvPVX( sv ); \
	memset(ref,0,sizeof(type)); \


struct __cnn {
	EVSockStruct(__cnn);
	
	uint32_t pending;
	uint32_t seq;
	U32      use_hash;
	HV      *reqs;
	HV      *spaces;
};
typedef struct __cnn TntCnn;


typedef struct {
	U32   id;
	SV   *name;
	AV   *fields;
	unpack_format f;
} TntIndex;

typedef struct {
	U32   id;
	SV   *name;
	
	AV   *fields;
	HV   *indexes;
	HV   *field;
	
	unpack_format f;
} TntSpace;

typedef struct {
	SV * cb;
	SV * wbuf;
	U32  use_hash;
	TntSpace *space;
	unpack_format *fmt;
	unpack_format f;
	char *call;
} TntCtx;

typedef struct {
	U32  id;
	char format;
} TntField;

void on_read(Cnn * self, size_t len) {
	debug("read %zu: %-.*s",len, (int)self->ruse, self->rbuf);
	dSP;
	
	ENTER;
	SAVETMPS;
	SV **sp1 = PL_stack_sp;
	
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
				
				int length = parse_reply( hv, rbuf, len+12, &ctx->f, ctx->use_hash ? ctx->space->fields : 0 );
				if (ctx->f.size && !ctx->f.nofree) {
					safefree(ctx->f.f);
				}
				if (length > 0) {
					(void) hv_stores(hv, "size", newSVuv(length));
				}
				
				if (ctx->cb) {
					ENTER;
					SAVETMPS;
					
					PUSHMARK(SP);
					EXTEND(SP, 1);
					//cwarn("read sp = %p (%d)",sp, PL_stack_sp - PL_stack_base);
				
					PUSHs( sv_2mortal(newRV_noinc( (SV *) hv )) );
					
					PUTBACK;
					
					call_sv( ctx->cb, G_DISCARD | G_VOID );
					//call_sv( cv, G_DISCARD | G_VOID | G_EVAL | G_KEEPERR );
					
					SvREFCNT_dec(ctx->cb);
					
					//PUTBACK;
				
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
	if (self->ruse > 0) {
		cwarn("move buf on %zu",self->ruse);
		memmove(self->rbuf,rbuf,self->ruse);
	}
	
	PL_stack_sp = sp1;
	
	FREETMPS;
	LEAVE;
}


MODULE = EV::Tarantool		PACKAGE = EV::Tarantool
PROTOTYPES: DISABLE
BOOT:
{
	I_EV_API ("EV::Tarantool");
	I_EV_SOCK_API("EV::Tarantool" );
}


void new(SV *pk, HV *conf)
	PPCODE:
		EVSockNew(TntCnn,on_read); //produce self, cnn, conf
		
		//cwarn("new     this: %p; iv[%d]: %p; self: %p; self->self: %p",ST(0), SvREFCNT(iv),iv, self, self->self);
		
		SV **key;
		
		self->reqs = newHV();
		
		self->use_hash = 1;
		if ((key = hv_fetchs(conf, "hash", 0)) ) self->use_hash = SvOK(*key) ? SvIV(*key) : 0;
		
		self->spaces = newHV();
		
		if ((key = hv_fetchs(conf, "spaces", 0)) && SvROK(*key)) {
			if (SvTYPE( SvRV( *key ) ) != SVt_PVHV) {
				croak("space config must be hash");
			}
			HV *sph = (HV *) SvRV(*key);
			HE *ent;
			char *nkey;
			STRLEN nlen;
			(void) hv_iterinit( sph );
			while ((ent = hv_iternext( sph ))) {
				char *name = HePV(ent, nlen);
				U32 id = atoi( name );
				//warn("hash key = %s; val = %s",name, SvPV_nolen(HeVAL(ent)));
				if (SvTYPE( SvRV( HeVAL(ent) ) ) != SVt_PVHV) {
					croak("space '%s' config must be hash", name);
				}
				
				HV *space = (HV *) SvRV(HeVAL(ent));
				
				SV *spcf = newSV( sizeof(TntSpace) );
				
				SvUPGRADE( spcf, SVt_PV );
				SvCUR_set(spcf,sizeof(TntSpace));
				SvPOKp_on(spcf);
				TntSpace * spc = (TntSpace *) SvPVX(spcf);
				memset(spc,0,sizeof(TntSpace));
				
				if ((key = hv_fetch( self->spaces,(char *)&id,sizeof(U32),0 )) && *key) {
					TntSpace *old  = (TntSpace *) SvPVX(*key);
					croak("Duplicate id '%f' for space %d. Already set by space %s", id, SvPV_nolen(old->name));
				}
				(void)hv_store( self->spaces,(char *)&id,sizeof(U32),spcf,0 );
				
				spc->id = id;
				spc->indexes = newHV();
				spc->field = newHV();
				
				if ((key = hv_fetch(space, "name", 4, 0)) && SvOK(*key)) {
					//warn("space %d have name '%s'",id,SvPV_nolen(*key));
					spc->name = newSVsv( *key );
					if ((key = hv_fetch( self->spaces,SvPV_nolen(spc->name),SvCUR(spc->name),0 )) && *key) {
						TntSpace *old  = (TntSpace *) SvPVX(*key);
						croak("Duplicate name '%s' for space %d. Already set by space %d", SvPV_nolen(spc->name), id, old->id);
					} else {
						(void)hv_store( self->spaces,SvPV_nolen(spc->name),SvCUR(spc->name),SvREFCNT_inc(spcf),0 );
					}
				}
				if ((key = hv_fetch(space, "types", 5, 0)) && SvROK(*key)) {
					if (SvTYPE( SvRV( *key ) ) != SVt_PVAV) croak("Types must be arrayref");
					AV *types = (AV *) SvRV(*key);
					int ix;
					spc->f.nofree = 1;
					spc->f.size = av_len(types)+1;
					spc->f.f = safemalloc( spc->f.size + 1 );
					spc->f.f[ spc->f.size ] = 0;
					spc->f.def = 'p';
					for(ix = 0; ix <= av_len(types); ix++){
						key = av_fetch( types,ix,0 );
						STRLEN tlen;
						char *type = SvPV(*key,tlen);
						if (tlen == 3 && strncasecmp( type,"STR",3 ) == 0) {
							spc->f.f[ix] = 'p';
						}
						else
						if (tlen == 3 && strncasecmp( type,"INT",3 ) == 0) {
							spc->f.f[ix] = 'i';
						}
						else
						if (tlen == 3 && strncasecmp( type,"NUM",3 ) == 0) {
							spc->f.f[ix] = 'I';
						}
						else
						if (tlen == 5 && strncasecmp( type,"NUM64",3 ) == 0) {
							spc->f.f[ix] = 'L';
						}
						else
						if (tlen == 5 && strncasecmp( type,"INT64",3 ) == 0) {
							spc->f.f[ix] = 'l';
						}
						else
						if (strncasecmp( type,"UTF",3 ) == 0) {
							spc->f.f[ix] = 'u';
						}
						else {
							warn("Unknown type: '%s' for field %d. Using STR",type, ix);
							spc->f.f[ix] = 'p';
						}
					}
					//cwarn("format: %-.*s",(int)spc->f.size,spc->f.f);
				}
				if ((key = hv_fetch(space, "fields", 6, 0)) && SvROK(*key)) {
					if (SvTYPE( SvRV( *key ) ) != SVt_PVAV) croak("Fields must be arrayref");
					SvREFCNT_inc(spc->fields = (AV *)SvRV(*key));
					int fid;
					AV *fields = (AV *)SvRV(*key);
					for(fid = 0; fid <= av_len(fields); fid++) {
						SV **f = av_fetch( fields,fid,0 );
						if (!f) croak("XXX");
						
						char format;
						if ( fid < spc->f.size ) {
							format = spc->f.f[fid];
						}
						else {
							format = 'p';
						}
						//cwarn("field: %p -> %p",*f, SvPVX(*f));
						HE *fhe = hv_fetch_ent(spc->field,*f,1,0);
						if (SvOK( HeVAL(fhe) )) {
							croak("Duplicate field name: '%s',",SvPV_nolen(HeVAL(fhe)));
						}
						else {
							dSVX( fldsv,fld,TntField );
							fld->id = fid;
							fld->format = format;
							hv_store(spc->field,SvPV_nolen(*f),sv_len(*f),fldsv, HeHASH(fhe));
						}
					}
				}
				if ((key = hv_fetch(space, "indexes", 7, 0)) && SvROK(*key)) {
					if (SvTYPE( SvRV( *key ) ) != SVt_PVHV) croak("Indexes must be hashref");
					HV *idxs = (HV*) SvRV(*key);
					(void) hv_iterinit( idxs );
					while ((ent = hv_iternext( idxs ))) {
						char *iname = HePV(ent, nlen);
						U32 iid = atoi( iname );
						//warn("index %d",iid);
						if (SvTYPE( SvRV( HeVAL(ent) ) ) != SVt_PVHV) croak("index '%s' config must be hash", iname);
						HV *index = (HV *) SvRV(HeVAL(ent));
						
						SV *idxcf = newSV( sizeof(TntIndex) );
						SvUPGRADE(idxcf, SVt_PV);
						SvCUR_set(idxcf,sizeof(TntIndex));
						SvPOKp_on(idxcf);
						TntIndex *idx  = (TntIndex *) SvPVX(idxcf);
						memset(idx,0,sizeof(TntIndex));
						
						idx->id = iid;
						if ((key = hv_fetch( spc->indexes,(char *)&iid,sizeof(U32),0 )) && *key) {
							TntIndex *old  = (TntIndex *) SvPVX(*key);
							croak("Duplicate id '%d' for index in space %d. Already set by index %s", iid, id, SvPV_nolen(old->name));
						}
						(void)hv_store( spc->indexes,(char *)&iid,sizeof(U32),idxcf,0 );
						
						if ((key = hv_fetch(index, "name", 4, 0)) && SvOK(*key)) {
							//warn("index %d have name '%s'",iid,SvPV_nolen(*key));
							SV *ixname = *key;
							idx->name = newSVsv( *key );
							if ((key = hv_fetch( spc->indexes,SvPV_nolen(idx->name),SvCUR(idx->name),0 )) && *key) {
								TntIndex *old  = (TntIndex *) SvPVX(*key);
								croak("Duplicate name '%s' for index %d in space %d. Already set by index %d", SvPV_nolen(idx->name), iid, id, old->id);
							} else {
								//warn("key %s not exists in %p", SvPV_nolen(ixname), spc->indexes);
								(void)hv_store( spc->indexes,SvPV_nolen(idx->name),SvCUR(idx->name),SvREFCNT_inc(idxcf),0 );
							}
						}
						if ((key = hv_fetch(index, "fields", 6, 0)) && SvROK(*key)) {
							if (SvTYPE( SvRV( *key ) ) != SVt_PVAV) croak("Index fields must be arrayref");
							SvREFCNT_inc(idx->fields = (AV *)SvRV(*key));
							AV *fields = (AV *) SvRV(*key);
							int ix;
							idx->f.nofree = 1;
							idx->f.size = av_len(fields)+1;
							idx->f.f = safemalloc(idx->f.size+1);
							idx->f.f[idx->f.size] = 0;
							idx->f.def = 'p';
							for (ix=0;ix <= av_len(fields); ix++) {
								SV **f = av_fetch( fields,ix,0 );
								if (!f) croak("XXX");
								HE *fhe = hv_fetch_ent(spc->field,*f,1,0);
								if (SvOK( HeVAL(fhe) )) {
									idx->f.f[ix] = ((TntField *)SvPVX( HeVAL(fhe) ))->format;
								}
								else {
									croak("Unknown field name: '%s' in index %d of space %d",SvPV_nolen( *f ), iid, id);
								}
							}
							//warn("index %d format (%zu): %-.*s",iid,idx->f.size,(int)idx->f.size,idx->f.f);
						}
					}
				}
				
			}
			
			//croak("XXX");
		}

		XSRETURN(1);


void DESTROY(SV *this)
	PPCODE:
		EVSockSelf(TntCnn);
		
		//cwarn("destroy this: %p; iv[%d]: %p; self: %p; self->self: %p",ST(0), SvREFCNT(SvRV(this)), SvRV(this), self, self->self);
		//SV * leak = newSV(1024);
		
		if (self->reqs) {
			//TODO
			SvREFCNT_dec(self->reqs);
		}
		if (self->spaces) {
			HE *ent;
			STRLEN nlen;
			(void) hv_iterinit( self->spaces );
			while ((ent = hv_iternext( self->spaces ))) {
				HE *he;
				TntSpace * spc = (TntSpace *) SvPVX( HeVAL(ent) );
				if (spc->name) {
					//cwarn("destroy space %s",SvPV_nolen(spc->name));
					if (spc->fields) SvREFCNT_dec(spc->fields);
					if (spc->field) {
						SvREFCNT_dec( spc->field );
					}
					if (spc->indexes) {
						(void) hv_iterinit( spc->indexes );
						while ((he = hv_iternext( spc->indexes ))) {
							TntIndex * idx = (TntIndex *) SvPVX( HeVAL(he) );
							if (idx->name) {
								//cwarn("destroy index %s in space %s",SvPV_nolen(idx->name), SvPV_nolen(spc->name));
								if (idx->f.size > 0) safefree(idx->f.f);
								if (idx->fields) SvREFCNT_dec(idx->fields);
								SvREFCNT_dec(idx->name);
								idx->name = 0;
							}
						}
						SvREFCNT_dec( spc->indexes );
					}
					SvREFCNT_dec(spc->name);
					spc->name = 0;
				}
				
			}
			
			SvREFCNT_dec(self->spaces);
		}
		EVSockDestroy(self);

void reqs(SV *this)
	PPCODE:
		EVSockSelf(TntCnn);
		ST(0) = sv_2mortal(newRV_inc((SV *)self->reqs));
		XSRETURN(1);

void spaces(SV *this)
	PPCODE:
		EVSockSelf(TntCnn);
		ST(0) = sv_2mortal(newRV_inc((SV *)self->spaces));
		XSRETURN(1);

void ping(SV *this, SV * cb)
	PPCODE:
		EVSockSelf(TntCnn);
		EVSockCheckConn(self);
		
		dSVX(ctxsv, ctx, TntCtx);
		ctx->call = "ping";
		
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
		EVSockSelf(TntCnn);
		SV *cb = ST(items-1);
		EVSockCheckConn(self);
		register uniptr p;
		
		//cwarn("lua sp = %p (%d)",sp, PL_stack_sp - PL_stack_base);
		
		U32 flags = 0;
		U32 iid = ++self->seq;
		HV *opt = 0;
		SV **key;
		
		if (items == 5) {
			opt = (HV *) SvRV(ST( 3 ));
			if ((key = hv_fetch(opt, "quiet", 5, 0)) && SvOK(*key)) flags |= TNT_FLAG_BOX_QUIET;
			if ((key = hv_fetch(opt, "nostore", 7, 0)) && SvOK(*key)) flags |= TNT_FLAG_NOT_STORE;
		}
		
		U32 ns;
		TntSpace *spc = 0;
		if (opt && (key = hv_fetch(opt,"space",5,0)) && SvOK(*key)) {
			SV *space = *key;
			if (SvIOK( space )) {
				ns = SvUV(space);
				if ((key = hv_fetch( self->spaces,(char *)&ns,sizeof(U32),0 )) && *key) {
					spc = (TntSpace*) SvPVX(*key);
				}
				else {
					warn("No space %d config",ns);
				}
			}
			else {
				if ((key = hv_fetch( self->spaces,SvPV_nolen(space),SvCUR(space),0 )) && *key) {
					spc = (TntSpace*) SvPVX(*key);
					ns = spc->id;
				}
				else {
					croak("Unknown space %s",SvPV_nolen(space));
				}
			}
		}
		
		dSVX(ctxsv, ctx, TntCtx); //newSV
		ctx->call = "lua";
		dUnpackFormat( format );
		
		if(spc) {
			ctx->space = spc;
			ctx->use_hash = self->use_hash;
			if (opt && (key = hv_fetch(opt, "hash", 4, 0)) ) ctx->use_hash = SvOK(*key) ? SvIV( *key ) : 0;
		}
		else {
			ctx->space = 0;
			ctx->use_hash = 0;
		}
		
		if (opt && (key = hv_fetch(opt,"out",3,0)) && *key) {
			//sv_dump(*key);
			//cwarn("extract out format '%s' ([%d] %p->%p) from %p for id %d",SvPV_nolen(*key), SvREFCNT(*key), *key, SvPVX(*key), opt, iid);
			dExtractFormatCopy( &ctx->f, (*key) );
			//cwarn("extract out format %p -> %p (%d)",SvPV_nolen(*key), ctx->f.f, ctx->f.nofree);
		}
		else
		if (spc) {
			memcpy(&ctx->f,&spc->f,sizeof(unpack_format));
		}
		else
		{
			ctx->f.size = 0;
			ctx->f.nofree = 1;
		}
		
		SvREFCNT_inc(ctx->cb = cb);
		
		(void) hv_store( self->reqs, (char*)&iid, sizeof(iid), ctxsv, 0 );
		
		//SV *idkey = sv_2mortal(newSVuv(iid));
		//(void) hv_store( self->reqs, SvPV_nolen(idkey), sv_len(idkey), ctxsv, 0 );
		
		debug("send lua with seq id %u",iid);
		
		if (opt && (key = hv_fetch(opt,"in",2,0)) && *key) {
			dExtractFormat2( format, *key );
		}
		
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

void select( SV *this, SV *space, AV * keys, ... )
	PPCODE:
		register uniptr p;
		
		EVSockSelf(TntCnn);
		SV *cb = ST(items-1);
		EVSockCheckConn(self);
		
		HV *opt = 0;
		
		U32 limit  = 0xffffffff;
		U32 offset = 0;
		U32 index  = 0;
		U32 flags  = 0;
		U32 ns     = 0;
		U32 iid = ++self->seq;
		
		unpack_format *fmt;
		dUnpackFormat( format );
		
		int k,i;
		SV **key;
		
		CHECK_NOT_CONN();
		
		TntSpace *spc = 0;
		TntIndex *idx = 0;
		
		if (SvIOK( space )) {
			ns = SvUV(space);
			if ((key = hv_fetch( self->spaces,(char *)&ns,sizeof(U32),0 )) && *key) {
				spc = (TntSpace*) SvPVX(*key);
			}
			else {
				//warn("No space %d config. Using without formats",ns);
			}
		}
		else {
			if ((key = hv_fetch( self->spaces,SvPV_nolen(space),SvCUR(space),0 )) && *key) {
				spc = (TntSpace*) SvPVX(*key);
				ns = spc->id;
			}
			else {
				croak("Unknown space %s",SvPV_nolen(space));
			}
		}
		
		dSVX(ctxsv, ctx, TntCtx); //newSV
		ctx->call = "select";
		ctx->space = spc;
		
		if (items == 5) {
			opt = (HV *) SvRV(ST( 3 ));
			if ((key = hv_fetch(opt, "index", 5, 0)) && SvOK(*key)) {
				if (SvIOK( *key )) {
					index = SvUV(*key);
				}
				else {
					if ((key = hv_fetch( spc->indexes,SvPV_nolen(*key),SvCUR(*key),0 )) && *key) {
						idx = (TntIndex*) SvPVX(*key);
						index = idx->id;
					}
					else {
						croak("Unknown index %s in space %d",SvPV_nolen(*key),ns);
					}
					
				}
			}
			if ((key = hv_fetchs(opt, "limit", 0)) && SvOK(*key)) limit = SvUV(*key);
			if ((key = hv_fetchs(opt, "offset", 0)) && SvOK(*key)) offset = SvUV(*key);
			
			if ((key = hv_fetchs(opt, "quiet", 0)) && SvOK(*key)) flags |= TNT_FLAG_BOX_QUIET;
			if ((key = hv_fetchs(opt, "nostore", 0)) && SvOK(*key)) flags |= TNT_FLAG_NOT_STORE;
			if ((key = hv_fetchs(opt, "hash", 0)) ) {
				ctx->use_hash = SvOK(*key) ? SvIV( *key ) : 0;
			}
		}
		else {
			ctx->f.size = 0;
		}
		if (!idx) {
			if ( spc && spc->indexes && (key = hv_fetch( spc->indexes,(char *)&index,sizeof(U32),0 )) && *key) {
				idx = (TntIndex*) SvPVX(*key);
			}
			else {
				//warn("No index %d config. Using without formats",index);
			}
		}
		if (!spc) ctx->use_hash = 0;
		
		
		if (opt && (key = hv_fetch(opt,"out",3,0)) && *key) {
			dExtractFormatCopy( &ctx->f, *key );
		}
		else
		if (spc) {
			memcpy(&ctx->f,&spc->f,sizeof(unpack_format));
		}
		else
		{
			ctx->f.size = 0;
		}
		
		if (opt && (key = hv_fetch(opt,"in",2,0)) && *key) {
			dExtractFormat2( format, *key );
			fmt = &format;
		}
		else
		if (idx) {
			fmt = &idx->f;
		}
		else
		{
			fmt = &format;
		}
		
		debug("opt = %p, c = %s",opt, SvPV_nolen(cb));
		
		/*
		SV *sv = newSV(
				( ( (
					sizeof( tnt_pkt_select_t ) +
					+ 4
					+ ( av_len(keys)+1 ) * ( 5 + 32 )
					+ 16
				) >> 5 ) << 5 ) + 0x20
		);
		SvUPGRADE( sv, SVt_PV );
		SvPOKp_on(sv);
		
		tnt_pkt_select_t *h = (tnt_pkt_select_t *) SvPVX(sv);
		*/
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
			if (!SvROK(t) || ( (SvTYPE(SvRV(t)) != SVt_PVAV) && (SvTYPE(SvRV(t)) != SVt_PVHV) ) ) {
				//SvREFCNT_dec(sv);
				//SvREFCNT_dec(ctxsv);
				sv_2mortal(sv);
				sv_2mortal(ctxsv);
				if (!ctx->f.nofree) safefree(ctx->f.f);
				croak("keys must be ARRAYREF of ARRAYREF or ARRAYREF of HASHREF");
			}
			AV *fields;
			if ((SvTYPE(SvRV(t)) == SVt_PVHV)) {
				if (!idx) {
					sv_2mortal(sv);
					sv_2mortal(ctxsv);
					if (!ctx->f.nofree) safefree(ctx->f.f);
					croak("Cannot use hash without index config");
				}
				HV *hf = (HV *) SvRV(t);
				fields = (AV *) sv_2mortal((SV *)newAV());
				HE *fl;
				int fcnt = HvTOTALKEYS(hf);
				for (k=0;k <= av_len( idx->fields );k++) {
					SV **f = av_fetch( idx->fields,k,0 );
					if (!f) croak("XXX");
					fl = hv_fetch_ent(hf,*f,0,0);
					if (fl && SvOK( HeVAL(fl) )) {
						fcnt--;
						av_push( fields, SvREFCNT_inc(HeVAL(fl)) );
					}
					else {
						break;
					}
				}
				if (fcnt != 0) {
					HV *used = (HV*)sv_2mortal((SV*)newHV());
					for (k=0;k <= av_len( idx->fields );k++) {
						SV **f = av_fetch( idx->fields,k,0 );
						fl = hv_fetch_ent(hf,*f,0,0);
						if (fl && SvOK( HeVAL(fl) )) {
							hv_store(used,SvPV_nolen(*f),sv_len(*f), &PL_sv_undef,0);
						}
						else {
							break;
						}
					}
					(void) hv_iterinit( hf );
					STRLEN nlen;
					HE *ent;
					while ((ent = hv_iternext( hf ))) {
						char *name = HePV(ent, nlen);
						if (!hv_exists(used,name,nlen)) {
							warn("query key = %s; val = %s in tuple %d could not be used in this index",name, SvPV_nolen(HeVAL(ent)), i);
						}
					}
				}
			}
			else {
				fields  = (AV *) SvRV(t);
			}
			
			*( p.i++ ) = htole32( av_len(fields) + 1 );
			
			for (k=0; k <= av_len(fields); k++) {
				SV *f = *av_fetch( fields, k, 0 );
				if ( !SvOK(f) || !sv_len(f) ) {
					*(p.c++) = 0;
				} else {
					uptr_sv_size( p, sv, 5 + sv_len(f) );
					uptr_field_sv_fmt( p, f, k < fmt->size ? fmt->f[k] : fmt->def );
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
		

void test(HV *hv)
	PPCODE:
			int fcnt;
			dTHX;
			HE *tmp;
			HvREHASH(hv);
			cwarn("tot: %zu",HvTOTALKEYS(hv));
			tmp = hv_fetch_ent(hv,sv_2mortal(newSVpvn("id",2)),0,0);
			cwarn("tmp = %p: %s %08x",tmp,SvPV_nolen(HeVAL(tmp)), HeHASH(tmp));
			tmp = hv_fetch_ent(hv,sv_2mortal(newSVpvn("a",1)),0,0);
			cwarn("tmp = %p: %s %08x",tmp,SvPV_nolen(HeVAL(tmp)), HeHASH(tmp));
			tmp = hv_fetch_ent(hv,sv_2mortal(newSVpvn("x",1)),0,0);
			cwarn("tmp = %p: %s %08x",tmp,SvPV_nolen(HeVAL(tmp)), HeHASH(tmp));
			tmp = hv_fetch_ent(hv,sv_2mortal(newSVpvn("z",1)),0,0);
			cwarn("tmp = %p: %s %08x",tmp,SvPV_nolen(HeVAL(tmp)), HeHASH(tmp));
			HE **ents = HvARRAY(hv);
			if (ents) {
				HE *const *const last = ents + HvMAX(hv);
				fcnt = last + 1 - ents;
				cwarn("cnt = %d",fcnt);
				do {
					cwarn("%p: %s %08x",*ents, *ents ? (SvPV_nolen(HeVAL(*ents))) : 0, *ents ? HeHASH(*ents) : 0);
					if (!*ents)
						--fcnt;
				} while (++ents <= last);
			}
		
	XSRETURN_UNDEF;

void insert( SV *this, SV *space, SV * t, ... )
	ALIAS:
		insert = TNT_OP_INSERT
		delete = TNT_OP_DELETE
	PPCODE:
		register uniptr p;
		EVSockSelf(TntCnn);
		SV *cb = ST(items-1);
		EVSockCheckConn(self);
		HV *opt = 0;
		
		U32 flags = 0;
		U32 ns;
		U32 iid = ++self->seq;
		
		unpack_format *fmt;
		dUnpackFormat( format );
		
		int k,i;
		SV **key;
		
		if (!SvROK(t) || ( (SvTYPE(SvRV(t)) != SVt_PVAV) && (SvTYPE(SvRV(t)) != SVt_PVHV) ) ) {
			croak("Tuple must be ARRAYREF or HASHREF");
		}
		
		TntSpace *spc = 0;
		TntIndex *idx = 0;
		
		
		if (SvIOK( space )) {
			ns = SvUV(space);
			if ((key = hv_fetch( self->spaces,(char *)&ns,sizeof(U32),0 )) && *key) {
				spc = (TntSpace*) SvPVX(*key);
			}
			else {
				warn("No space %d config. Using without formats",ns);
			}
		}
		else {
			if ((key = hv_fetch( self->spaces,SvPV_nolen(space),SvCUR(space),0 )) && *key) {
				spc = (TntSpace*) SvPVX(*key);
				ns = spc->id;
			}
			else {
				croak("Unknown space %s",SvPV_nolen(space));
			}
		}
		
		if (!spc && (SvTYPE(SvRV(t)) == SVt_PVHV)) {
			croak("Cannot use hash without space config");
		}
		dSVX(ctxsv, ctx, TntCtx); //newSV
		
		ctx->use_hash = self->use_hash;
		ctx->space = spc;
		
		if (items == 5) {
			opt = (HV *) SvRV(ST( 3 ));
			
			if ((key = hv_fetch(opt, "return", 6, 0)) && SvOK(*key)) flags |= TNT_FLAG_RETURN;
			if ((key = hv_fetch(opt, "ret", 3, 0)) && SvOK(*key)) flags |= TNT_FLAG_RETURN;
			if ((key = hv_fetch(opt, "add", 3, 0)) && SvOK(*key)) flags |= TNT_FLAG_ADD;
			if ((key = hv_fetch(opt, "replace", 7, 0)) && SvOK(*key)) flags |= TNT_FLAG_REPLACE;
			if ((key = hv_fetch(opt, "rep", 3, 0)) && SvOK(*key)) flags |= TNT_FLAG_REPLACE;
			if ((key = hv_fetch(opt, "quiet", 5, 0)) && SvOK(*key)) flags |= TNT_FLAG_BOX_QUIET;
			if ((key = hv_fetch(opt, "nostore", 7, 0)) && SvOK(*key)) flags |= TNT_FLAG_NOT_STORE;
			if ((key = hv_fetch(opt, "hash", 4, 0)) ) ctx->use_hash = SvOK(*key) ? SvIV( *key ) : 0;
		}
		else {
			ctx->f.size = 0;
		}
		
		if (!spc) ctx->use_hash = 0;
		
		if (opt && (key = hv_fetch(opt,"out",3,0)) && *key) {
			dExtractFormatCopy( &ctx->f, *key );
		}
		else
		if (spc) {
			memcpy(&ctx->f,&spc->f,sizeof(unpack_format));
		}
		else
		{
			ctx->f.size = 0;
		}
		
		if (opt && (key = hv_fetch(opt,"in",2,0)) && *key) {
			dExtractFormat2( format, *key );
			fmt = &format;
		}
		else
		if (spc) {
			fmt = &spc->f;
		}
		else
		{
			fmt = &format;
		}
		
		debug("opt = %p, c = %s",opt, SvPV_nolen(cb));
		SV *sv = newSVpvn("",0);
		
		tnt_pkt_insert_t *h = (tnt_pkt_insert_t *)
			SvGROW( sv, 
				( ( (
					sizeof( tnt_pkt_insert_t ) +
					+ 4
					+ (  (SvTYPE(SvRV(t)) == SVt_PVHV) ? HvTOTALKEYS((HV*)SvRV(t)) : av_len((AV*)SvRV(t))+1 ) * ( 5 + 32 )
					+ 16
				) >> 5 ) << 5 ) + 0x20
			);
		
		p.c = (char *)(h+1);
		
		AV *fields;
		if ((SvTYPE(SvRV(t)) == SVt_PVHV)) {
			HV *hf = (HV *) SvRV(t);
			HE *fl;
			fields = (AV *) sv_2mortal((SV *)newAV());
			int fcnt = HvTOTALKEYS(hf);
			for (k=0; k <= av_len( spc->fields );k++) {
				SV **f = av_fetch( spc->fields,k,0 );
				if (!f) croak("XXX");
				fl = hv_fetch_ent(hf,*f,0,0);
				if (fl && SvOK( HeVAL(fl) )) {
					fcnt--;
					av_push( fields, SvREFCNT_inc(HeVAL(fl)) );
				}
				else {
					av_push( fields, &PL_sv_undef );
				}
			}
			if ((key = hv_fetch(hf,"",0,0)) && SvROK(*key)) {
				AV *tail = (AV *) SvRV( *key );
				fcnt--;
				for (k=0; k <= av_len( tail ); k++) {
					key = av_fetch(tail,k,0);
					av_push( fields, SvREFCNT_inc(*key) );
				}
			}
			if (fcnt != 0) {
				HV *used = (HV*)sv_2mortal((SV*)newHV());
				for (k=0; k <= av_len( spc->fields );k++) {
					SV **f = av_fetch( spc->fields,k,0 );
					fl = hv_fetch_ent(hf,*f,0,0);
					if (fl && SvOK( HeVAL(fl) )) {
						hv_store(used,SvPV_nolen(*f),sv_len(*f), &PL_sv_undef,0);
					}
				}
				if ((key = hv_fetch(hf,"",0,0)) && SvROK(*key)) {
					hv_store(used,"",0, &PL_sv_undef,0);
				}
				(void) hv_iterinit( hf );
				STRLEN nlen;
				HE *ent;
				while ((ent = hv_iternext( hf ))) {
					char *name = HePV(ent, nlen);
					if (!hv_exists(used,name,nlen)) {
						warn("tuple key = %s; val = %s could not be used in space %d",name, SvPV_nolen(HeVAL(ent)), spc->id);
					}
				}
			}
		}
		else {
			fields  = (AV *) SvRV(t);
		}
		
		*(p.i++) = htole32( av_len(fields) + 1 );
		
		for (k=0; k <= av_len(fields); k++) {
			key = av_fetch( fields, k, 0 );
			if (key && *key) {
				if ( !SvOK(*key) || !sv_len(*key) ) {
					*(p.c++) = 0;
				} else {
					uptr_sv_size( p, sv, 5 + sv_len(*key) );
					uptr_field_sv_fmt( p, *key, k < fmt->size ? fmt->f[k] : fmt->def );
				}
			}
			else {
				*(p.c++) = 0;
			}
		}
		
		SvCUR_set( sv, p.c - SvPVX(sv) );
		h = (tnt_pkt_insert_t *) SvPVX( sv ); // for sure
		h->type   = htole32( ix );
		h->reqid  = htole32( iid );
		h->space  = htole32( ns );
		h->flags  = htole32( flags );
		h->len    = htole32( SvCUR(sv) - sizeof( tnt_hdr_t ) );
		
		SvREFCNT_inc(ctx->cb = cb);
		
		ctx->wbuf = sv;
		(void) hv_store( self->reqs, (char*)&iid, sizeof(iid), ctxsv, 0 );
		
		++self->pending;
		
		do_write( &self->cnn,SvPVX(ctx->wbuf), SvCUR(ctx->wbuf));
		
		XSRETURN_UNDEF;
		
void update( SV *this, SV *space, SV * t, AV *ops, ... )
	PPCODE:
		register uniptr p;
		EVSockSelf(TntCnn);
		SV *cb = ST(items-1);
		EVSockCheckConn(self);
		HV *opt = 0;

		U32 flags = 0;
		U32 ns;
		U32 iid = ++self->seq;
		
		U32 index = 0;
		
		unpack_format *fmt;
		dUnpackFormat( format );
		
		int k,i;
		SV **key,**val;
		
		if (!SvROK(t) || ( (SvTYPE(SvRV(t)) != SVt_PVAV) && (SvTYPE(SvRV(t)) != SVt_PVHV) ) ) {
			croak("Tuple must be ARRAYREF or HASHREF");
		}
		
		TntSpace *spc = 0;
		TntIndex *idx = 0;
		
		
		if (SvIOK( space )) {
			ns = SvUV(space);
			if ((key = hv_fetch( self->spaces,(char *)&ns,sizeof(U32),0 )) && *key) {
				spc = (TntSpace*) SvPVX(*key);
			}
			else {
				warn("No space %d config. Using without formats",ns);
			}
		}
		else {
			if ((key = hv_fetch( self->spaces,SvPV_nolen(space),SvCUR(space),0 )) && *key) {
				spc = (TntSpace*) SvPVX(*key);
				ns = spc->id;
			}
			else {
				croak("Unknown space %s",SvPV_nolen(space));
			}
		}
		
		if ( spc && spc->indexes && (key = hv_fetch( spc->indexes,(char *)&index,sizeof(U32),0 )) && *key) {
			idx = (TntIndex*) SvPVX(*key);
		}
		
		if (!idx && (SvTYPE(SvRV(t)) == SVt_PVHV)) {
			croak("Cannot use hash without index config");
		}
		dSVX(ctxsv, ctx, TntCtx); //newSV
		
		ctx->use_hash = self->use_hash;
		ctx->space = spc;
		
		if (items == 6) {
			opt = (HV *) SvRV(ST( 4 ));
			
			if ((key = hv_fetch(opt, "return", 6, 0)) && SvOK(*key)) flags |= TNT_FLAG_RETURN;
			if ((key = hv_fetch(opt, "ret", 3, 0)) && SvOK(*key)) flags |= TNT_FLAG_RETURN;
			if ((key = hv_fetch(opt, "nostore", 7, 0)) && SvOK(*key)) flags |= TNT_FLAG_NOT_STORE;
			if ((key = hv_fetch(opt, "hash", 4, 0)) ) ctx->use_hash = SvOK(*key) ? SvIV( *key ) : 0;
		}
		else {
			ctx->f.size = 0;
		}
		
		if (!spc) ctx->use_hash = 0;
		
		if (opt && (key = hv_fetch(opt,"out",3,0)) && *key) {
			dExtractFormatCopy( &ctx->f, *key );
		}
		else
		if (spc) {
			memcpy(&ctx->f,&spc->f,sizeof(unpack_format));
		}
		else
		{
			ctx->f.size = 0;
		}
		
		if (opt && (key = hv_fetch(opt,"in",2,0)) && *key) {
			dExtractFormat2( format, *key );
			fmt = &format;
		}
		else
		if (spc) {
			fmt = &spc->f;
		}
		else
		{
			fmt = &format;
		}
		
		size_t bufsize = 
				( ( (
					sizeof( tnt_pkt_update_t ) +
					+ 4
					+ ( (SvTYPE(SvRV(t)) == SVt_PVHV) ? HvTOTALKEYS((HV*)SvRV(t)) : av_len((AV*)SvRV(t))+1 ) * ( 5 + 32 )
					+ 4
					+ ( av_len(ops)+1 ) * ( 4 + 1 + 5 + 32 )
					+ 128
				) >> 5 ) << 5 ) + 0x20;
		
		SV *sv = newSV(bufsize);
		SvUPGRADE( sv, SVt_PV );
		SvPOKp_on(sv);
		
		tnt_pkt_update_t *h = (tnt_pkt_update_t *) SvPVX(sv);
		
		p.c = (char *)(h+1);
		
		AV *fields;
		if ((SvTYPE(SvRV(t)) == SVt_PVHV)) {
			HV *hf = (HV *) SvRV(t);
			HE *fl;
			fields = (AV *) sv_2mortal((SV *)newAV());
			int fcnt = HvTOTALKEYS(hf);
			for (k=0; k <= av_len( idx->fields );k++) {
				SV **f = av_fetch( idx->fields,k,0 );
				if (!f) croak("XXX");
				fl = hv_fetch_ent(hf,*f,0,0);
				if (fl && SvOK( HeVAL(fl) )) {
					fcnt--;
					av_push( fields, SvREFCNT_inc(HeVAL(fl)) );
				}
				else {
					av_push( fields, &PL_sv_undef );
				}
			}
			if (fcnt != 0) {
				warn("fcnt != 0");
				HV *used = (HV*)sv_2mortal((SV*)newHV());
				for (k=0; k <= av_len( idx->fields );k++) {
					SV **f = av_fetch( idx->fields,k,0 );
					fl = hv_fetch_ent(hf,*f,0,0);
					if (fl && SvOK( HeVAL(fl) )) {
						hv_store(used,SvPV_nolen(*f),sv_len(*f), &PL_sv_undef,0);
					}
				}
				if ((key = hv_fetch(hf,"",0,0)) && SvROK(*key)) {
					hv_store(used,"",0, &PL_sv_undef,0);
				}
				(void) hv_iterinit( hf );
				STRLEN nlen;
				HE *ent;
				while ((ent = hv_iternext( hf ))) {
					char *name = HePV(ent, nlen);
					if (!hv_exists(used,name,nlen)) {
						warn("tuple key = %s; val = %s could not be used in index %d",name, SvPV_nolen(HeVAL(ent)), idx->id);
					}
				}
			}
		}
		else {
			fields  = (AV *) SvRV(t);
		}
		
		*( p.i++ ) = htole32( av_len(fields) + 1 );
		for (k=0; k <= av_len(fields); k++) {
			SV **f = av_fetch( fields, k, 0 );
			if ( !f || !SvOK(*f) || !sv_len(*f) ) {
				p.c += varint_write( p.c, 0 );
			} else {
				uptr_sv_size( p, sv, 5 + sv_len(*f) );
				uptr_field_sv_fmt( p, *f, k < format.size ? format.f[k] : format.def );
			}
		}
		
		AV *aop;
		
		*( p.i++ ) = htole32( av_len(ops) + 1 );
		
		for (k = 0; k <= av_len( ops ); k++) {
			val = av_fetch( ops, k, 0 );
			if (!*val || !SvROK( *val ) || SvTYPE( SvRV(*val) ) != SVt_PVAV )
				croak("Single update operation byst be arrayref");
				//croak("Wrong update operation format: %s", val ? SvPV_nolen(*val) : "undef");
			aop = (AV *)SvRV(*val);
			
			if ( av_len( aop ) < 1 ) croak("Too short operation argument list");
			
			key = av_fetch( aop, 0, 0 );
			char field_format = 0;
			if (SvUOK(*key)) {
				*( p.i++ ) = htole32( SvUV( *key ) );
				if ( spc && SvUV( *key ) < spc->f.size ) {
					field_format = spc->f.f[ SvUV( *key ) ];
				}
			}
			else {
				HE *fhe = hv_fetch_ent(spc->field,*key,1,0);
				if (fhe && SvOK( HeVAL(fhe) )) {
					TntField *fld = (TntField *) SvPVX( HeVAL(fhe) );
					field_format = fld->format;
					*( p.i++ ) = htole32( fld->id );
				}
				else {
					croak("Unknown field name: '%s' in space %d",SvPV_nolen( *key ), ns);
				}
			}
			
			
			char *opname = SvPV_nolen( *av_fetch( aop, 1, 0 ) );
			
			U8     opcode = 0;
			
			// Assign and insert allow formats. by default: p
			// Splice always 'p'
			// num ops always force format l or i (32 or 64), depending on size
			
			switch (*opname) {
				case '#': //delete
					*( p.c++ ) = TNT_UPDATE_DELETE;
					*( p.c++ ) = 0;
					break;
				case '=': //set
					*( p.c++ ) =  TNT_UPDATE_ASSIGN;
					val = av_fetch( aop, 2, 0 );
					if (val && *val && SvOK(*val)) {
						uptr_sv_size( p,sv, 5 + sv_len(*val));
						uptr_field_sv_fmt( p, *val, av_len(aop) > 2 ? *SvPV_nolen( *av_fetch( aop, 3, 0 ) ) : 'p' );
					} else {
						warn("undef in assign");
						*( p.c++ ) = 0;
					}
					break;
				case '!': // insert
					//if ( av_len( aop ) < 2 ) croak("Too short operation argument list for %c. Need 3 or 4, have %d", *opname, av_len( aop ) );
					*( p.c++ ) = TNT_UPDATE_INSERT;
					val = av_fetch( aop, 2, 0 );
					if (val && *val && SvOK(*val)) {
						uptr_sv_size( p,sv, 5 + sv_len(*val));
						uptr_field_sv_fmt( p, *val, av_len(aop) > 2 ? *SvPV_nolen( *av_fetch( aop, 3, 0 ) ) : 'p' );
					} else {
						warn("undef in insert");
						*( p.c++ ) = 0;
					}
					break;
				case ':': //splice
					//if ( av_len( aop ) < 4 ) croak("Too short operation argument list for %c. Need 5, have %d", *opname, av_len( aop ) );
					
					*( p.c++ ) = TNT_UPDATE_SPLICE;
					
					val = av_fetch( aop, 4, 0 );
					
					uptr_sv_size( p,sv, 15 + sv_len(*val));
					
					p.c = varint( p.c, 1+4 + 1+4  + varint_size( sv_len(*val) ) + sv_len(*val) );
					
					*(p.c++) = 4;
					*(p.i++) = (U32)SvIV( *av_fetch( aop, 2, 0 ) );
					*(p.c++) = 4;
					*(p.i++) = (U32)SvIV( *av_fetch( aop, 3, 0 ) );
					
					uptr_field_sv_fmt( p, *val, 'p' );
					break;
				case '+': //add
					opcode = TNT_UPDATE_ADD;
					break;
				case '&': //and
					opcode = TNT_UPDATE_AND;
					break;
				case '|': //or
					opcode = TNT_UPDATE_OR;
					break;
				case '^': //xor
					opcode = TNT_UPDATE_XOR;
					break;
				default:
					croak("Unknown operation: %c", *opname);
			}
			if (opcode) { // Arith ops
				if ( av_len( aop ) < 2 ) croak("Too short operation argument list for %c", *opname);
				
				*( p.c++ ) = opcode;
				
				unsigned long long v = SvUV( *av_fetch( aop, 2, 0 ) );
				if (v > 0xffffffff) {
					*( p.c++ ) = 8;
					*( p.q++ ) = (U64) v;
				} else {
					*( p.c++ ) = 4;
					*( p.i++ ) = (U32) v;
				}
			}
		}
		SvCUR_set( sv, p.c - SvPVX(sv) );
		
		h = (tnt_pkt_insert_t *) SvPVX( sv ); // for sure
		
		h->type   = htole32( TNT_OP_UPDATE );
		h->reqid  = htole32( iid );
		h->space  = htole32( ns );
		h->flags  = htole32( flags );
		h->len    = htole32( SvCUR(sv) - sizeof( tnt_hdr_t ) );
		
		SvREFCNT_inc(ctx->cb = cb);
		
		ctx->wbuf = sv;
		(void) hv_store( self->reqs, (char*)&iid, sizeof(iid), ctxsv, 0 );
		
		++self->pending;
		
		do_write( &self->cnn,SvPVX(ctx->wbuf), SvCUR(ctx->wbuf));
		
		XSRETURN_UNDEF;
