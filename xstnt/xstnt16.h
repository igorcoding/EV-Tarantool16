#define MP_SOURCE 1
#include <string.h>
#include "xsmy.h"
#include "msgpuck.h"
#include "defines.h"

/* types */
enum tp_type {
	TP_NIL = MP_NIL,
	TP_UINT = MP_UINT,
	TP_INT = MP_INT,
	TP_STR = MP_STR,
	TP_BIN = MP_BIN,
	TP_ARRAY = MP_ARRAY,
	TP_MAP = MP_MAP,
	TP_BOOL = MP_BOOL,
	TP_FLOAT = MP_FLOAT,
	TP_DOUBLE = MP_DOUBLE,
	TP_EXT = MP_EXT
};

/* header */
enum tp_header_key_t {
	TP_CODE = 0x00,
	TP_SYNC = 0x01
};

/* request body */
enum tp_body_key_t {
	TP_SPACE = 0x10,
	TP_INDEX = 0x11,
	TP_LIMIT = 0x12,
	TP_OFFSET = 0x13,
	TP_ITERATOR = 0x14,
	TP_KEY = 0x20,
	TP_TUPLE = 0x21,
	TP_FUNCTION = 0x22,
	TP_USERNAME = 0x23
};

/* response body */
enum tp_response_key_t {
	TP_DATA = 0x30,
	TP_ERROR = 0x31
};

/* request types */
enum tp_request_type {
	TP_SELECT = 1,
	TP_INSERT = 2,
	TP_REPLACE = 3,
	TP_UPDATE = 4,
	TP_DELETE = 5,
	TP_CALL = 6,
	TP_AUTH = 7,
	TP_PING = 64
};

static const uint32_t SCRAMBLE_SIZE = 20;

/* Tnt legacy structures */

typedef struct {
	size_t  size;
	char   *f;
	int     nofree;
	char    def;
} unpack_format;


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
	ev_timer t;
	uint32_t id;
	void *self;
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

#define dUnpackFormat(fvar) unpack_format fvar; fvar.f = ""; fvar.nofree = 1; fvar.size = 0; fvar.def  = 'p'

static TntIndex * evt_find_index(TntSpace * spc, SV **key) {
	if (SvIOK( *key )) {
		int iid = SvUV(*key);
		if ((key = hv_fetch( spc->indexes,(char *)&iid,sizeof(U32),0 )) && *key) {
			return (TntIndex *) SvPVX(*key);
		}
		else {
			//warn("Unknown index %d in space %d",iid,spc->id);
			return NULL;
		}
	}
	else {
		if ((key = hv_fetch( spc->indexes,SvPV_nolen(*key),SvCUR(*key),0 )) && *key) {
			return (TntIndex*) SvPVX(*key);
		}
		else {
			return NULL;
			//croak("Unknown index %s in space %d",SvPV_nolen(*key),spc->id);
		}
	}

}

static TntSpace * evt_find_space(SV *space, HV*spaces) {
	U32 ns;
	SV **key;
	if (SvIOK( space )) {
		ns = SvUV(space);
		if ((key = hv_fetch( spaces,(char *)&ns,sizeof(U32),0 )) && *key) {
			return (TntSpace*) SvPVX(*key);
		}
		else {
			warn("No space %d config. Creating dummy space",ns);
			SV *spcf = newSV( sizeof(TntSpace) );
			SvUPGRADE( spcf, SVt_PV );
			SvCUR_set(spcf,sizeof(TntSpace));
			SvPOKp_on(spcf);
			TntSpace * spc = (TntSpace *) SvPVX(spcf);
			memset(spc,0,sizeof(TntSpace));

			spc->id = ns;
			spc->name = newSVpvf( "%u",ns );
			spc->f.def = 'p';

			(void)hv_store( spaces, (char *)&ns,sizeof(U32),spcf,0 );
			(void)hv_store( spaces, SvPV_nolen(spc->name),SvLEN(spc->name),SvREFCNT_inc(spcf),0 );
			return spc;
		}
	}
	else {
		if ((key = hv_fetch( spaces,SvPV_nolen(space),SvCUR(space),0 )) && *key) {
			return (TntSpace*) SvPVX(*key);
		}
		else {
			//return NULL;
			croak("Unknown space %s",SvPV_nolen(space));
			return 0;
		}
	}
}

static void configure_spaces(HV *dest, SV * src) {
	SV **key;
	if (SvTYPE( SvRV( src ) ) != SVt_PVHV) {
		croak("Space config must be hash");
	}
	HV *sph = (HV *) SvRV(src);
	HE *ent;
	STRLEN nlen;
	(void) hv_iterinit( sph );
	while ((ent = hv_iternext( sph ))) {
		char *name = HePV(ent, nlen);
		U32 id = atoi( name );
		//cwarn("hash id = %d; key = %s; val = %s",id,name, SvPV_nolen(HeVAL(ent)));
		if (SvTYPE( SvRV( HeVAL(ent) ) ) != SVt_PVHV) {
			croak("Space '%s' config must be hash", name);
		}
		if ((key = hv_fetch( dest,(char *)&id,sizeof(U32),0 )) && *key) {
			TntSpace *old  = (TntSpace *) SvPVX(*key);
			croak("Duplicate id '%f' for space %d. Already set by space %s", id, SvPV_nolen(old->name));
		}

		HV *space = (HV *) SvRV(HeVAL(ent));

		SV *spcf = newSV( sizeof(TntSpace) );

		SvUPGRADE( spcf, SVt_PV );
		SvCUR_set(spcf,sizeof(TntSpace));
		SvPOKp_on(spcf);
		TntSpace * spc = (TntSpace *) SvPVX(spcf);
		memset(spc,0,sizeof(TntSpace));

		(void)hv_store( dest,(char *)&id,sizeof(U32),spcf,0 );

		spc->id = id;
		spc->indexes = newHV();
		spc->field   = newHV();

		if ((key = hv_fetch(space, "name", 4, 0)) && SvOK(*key)) {
			//cwarn("space %d have name '%s'",id,SvPV_nolen(*key));
			spc->name = newSVsv( *key );
			if ((key = hv_fetch( dest,SvPV_nolen(spc->name),SvCUR(spc->name),0 )) && *key) {
				TntSpace *old  = (TntSpace *) SvPVX(*key);
				croak("Duplicate name '%s' for space %d. Already set by space %d", SvPV_nolen(spc->name), id, old->id);
			} else {
				(void)hv_store( dest,SvPV_nolen(spc->name),SvCUR(spc->name),SvREFCNT_inc(spcf),0 );
			}
		} else {
			spc->name = newSVpvf("unnamed:%d",id);
		}
		if ((key = hv_fetch(space, "types", 5, 0)) && SvROK(*key)) {
			if (SvTYPE( SvRV( *key ) ) != SVt_PVAV) croak("Types must be arrayref");
			AV *types = (AV *) SvRV(*key);
			int ix;
			spc->f.nofree = 1;
			spc->f.size = av_len(types)+1;
			spc->f.f = safemalloc( spc->f.size + 1 );
			//cwarn("alloc: %p",spc->f.f);
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
				if (tlen == 5 && strncasecmp( type,"NUM64",5 ) == 0) {
					spc->f.f[ix] = 'L';
				}
				else
				if (tlen == 5 && strncasecmp( type,"INT64",5 ) == 0) {
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
				if (!f) croak("Bad field entry for space %d at offset %d", id, fid);

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
					(void) hv_store(spc->field,SvPV_nolen(*f),sv_len(*f),fldsv, HeHASH(fhe));
				}
			}
		}
		if (
			( (key = hv_fetchs(space, "indexes",0)) && SvROK(*key) )
			||
			( (key = hv_fetchs(space, "index", 0)) && SvROK(*key) )
			||
			( (key = hv_fetchs(space, "indices", 0)) && SvROK(*key) )
		) {
			if (SvTYPE( SvRV( *key ) ) != SVt_PVHV) croak("Indexes must be hashref");
			HV *idxs = (HV*) SvRV(*key);
			(void) hv_iterinit( idxs );
			while ((ent = hv_iternext( idxs ))) {
				char *iname = HePV(ent, nlen);
				U32 iid = atoi( iname );
				if (SvTYPE( SvRV( HeVAL(ent) ) ) != SVt_PVHV) croak("Index '%s' config must be hash", iname);
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
					//cwarn("index %d have name '%s'",iid,SvPV_nolen(*key));
					idx->name = newSVsv( *key );
					if ((key = hv_fetch( spc->indexes,SvPV_nolen(idx->name),SvCUR(idx->name),0 )) && *key) {
						TntIndex *old  = (TntIndex *) SvPVX(*key);
						croak("Duplicate name '%s' for index %d in space %d. Already set by index %d", SvPV_nolen(idx->name), iid, id, old->id);
					} else {
						//warn("key %s not exists in %p", SvPV_nolen(ixname), spc->indexes);
						(void)hv_store( spc->indexes,SvPV_nolen(idx->name),SvCUR(idx->name),SvREFCNT_inc(idxcf),0 );
					}
				}
				if ((key = hv_fetch(index, "fields", 6, 0))) {
					SV* newkey = 0;
					if (! SvROK(*key) ) {
						AV *av = newAV();
						av_store( av, 0, *key);
						newkey = sv_2mortal(newRV_noinc( (SV*) av));
					}
					if (! newkey || ! SvROK(newkey)) newkey = (SV*) *key;
					if ((SvROK(newkey) && SvTYPE( SvRV( newkey ) ) == SVt_PVAV)) {
						SvREFCNT_inc(idx->fields = (AV *)SvRV(newkey));
						AV *fields = (AV *) SvRV(newkey);
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
						//cwarn("index %d format (%zu): %-.*s",iid,idx->f.size,(int)idx->f.size,idx->f.f);
					} else {
						croak("Index fields mast be array");
					}
				}
			}
		} else {
			croak("Space %d requires at least one index", id);
		}

	}
}

static void destroy_spaces(HV *spaces) {
	HE *ent;
	(void) hv_iterinit( spaces );
	while ((ent = hv_iternext( spaces ))) {
		HE *he;
		TntSpace * spc = (TntSpace *) SvPVX( HeVAL(ent) );
		if (spc->name) {
			//cwarn("destroy space %d:%s",spc->id,SvPV_nolen(spc->name));
			if (spc->fields) SvREFCNT_dec(spc->fields);
			if (spc->field) {
				SvREFCNT_dec( spc->field );
			}
			if (spc->indexes) {
				//cwarn("des idxs, refs = %d", SvREFCNT( spc->indexes ));
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
			if (spc->f.size) {
				safefree(spc->f.f);
			}
		}

	}
	SvREFCNT_dec(spaces);
}

#define CHECK_PACK_FORMAT(src) \
	STMT_START { \
				char *p = src;\
				while(*p) { \
					switch(*p) { \
						case 'l':case 'L': \
							if (!HAS_LL) { croak_cb(cb,"Int64 support was not compiled in"); break; } \
						case 'i':case 'I': \
						case 's':case 'S': \
						case 'c':case 'C': \
						case 'p':case 'u': \
							p++; break; \
						default: \
							croak_cb(cb,"Unknown pattern in format: %c", *p); \
					} \
				} \
	} STMT_END

#define dExtractFormat2(fvar,src) STMT_START {                  \
			if ( SvOK(src) && SvPOK(src) ) {                 \
				fvar.f = SvPVbyte(src, fvar.size);               \
				CHECK_PACK_FORMAT( fvar.f );                         \
			}                                                        \
			else if (!SvOK( src )) {}                            \
			else {                                                   \
				croak_cb(cb,"Usage { .. in => 'fmtstring', out => 'fmtstring' .. }");   \
			}                                                        \
	} STMT_END



#define dExtractFormatCopy(fvar,src) STMT_START {                  \
			if ( SvOK(src) && SvPOK(src) ) {                 \
				(fvar)->f = SvPVbyte(src, (fvar)->size);               \
				CHECK_PACK_FORMAT( (fvar)->f );                         \
				(fvar)->f = safecpy((fvar)->f,(fvar)->size); \
				(fvar)->nofree = 0; \
			}                                                        \
			else if (!SvOK( src )) {}                            \
			else {                                                   \
				croak_cb(cb,"Usage { .. in => 'fmtstring', out => 'fmtstring' .. }");   \
			}                                                        \
	} STMT_END

#define evt_opt_out(opt,ctx,spc) STMT_START { \
	if (opt && (key = hv_fetchs(opt,"out",0)) && *key) { \
		dExtractFormatCopy( &ctx->f, *key ); \
	}\
	else\
	if (spc) {\
		memcpy(&ctx->f,&spc->f,sizeof(unpack_format));\
	}\
	else\
	{\
		ctx->f.size = 0;\
	}\
} STMT_END

#define evt_opt_in(opt,ctx,idx) STMT_START { \
	if (opt && (key = hv_fetchs(opt,"in",0)) && *key) { \
		dExtractFormat2( format, *key ); \
		fmt = &format; \
	} \
	else \
	if (idx) { \
		fmt = &idx->f; \
	} \
	else \
	{ \
		fmt = &format; \
	} \
} STMT_END

#define field_sv_fmt( dest, src, format )                                        \
	STMT_START {                                                                 \
		switch( format ) {                                                       \
			case 'l': dest = mp_encode_int  ( dest, (U64) SvIV( src ) );  break; \
			case 'L': dest = mp_encode_uint ( dest, (U64) SvUV( src ) );  break; \
			case 'i': dest = mp_encode_int  ( dest, (U32) SvIV( src ) );  break; \
			case 'I': dest = mp_encode_uint ( dest, (U32) SvUV( src ) );  break; \
			case 's': dest = mp_encode_int  ( dest, (U16) SvIV( src ) );  break; \
			case 'S': dest = mp_encode_uint ( dest, (U16) SvUV( src ) );  break; \
			case 'c': dest = mp_encode_int  ( dest, (U8)  SvIV( src ) );  break; \
			case 'C': dest = mp_encode_uint ( dest, (U8)  SvUV( src ) );  break; \
			case 'p': case 'u':                                                  \
				dest = mp_encode_str(dest, SvPV_nolen(src), sv_len(src)); break; \
			default:                                                             \
				croak_cb(cb,"Unsupported format: %c", format);                   \
		}                                                                        \
	} STMT_END


static AV * hash_to_array_fields(HV * hf, AV *fields, SV * cb) {
	AV *rv = (AV *) sv_2mortal((SV *)newAV());
	int fcnt = HvTOTALKEYS(hf);
	int k;

	SV **f;
	HE *fl;

	for (k=0; k <= av_len( fields );k++) {
		f = av_fetch( fields,k,0 );
		if (unlikely(!f)) {
			croak_cb(cb,"Missing field %d entry", k);
		}
		fl = hv_fetch_ent(hf,*f,0,0);
		if (fl && SvOK( HeVAL(fl) )) {
			fcnt--;
			av_push( rv, SvREFCNT_inc(HeVAL(fl)) );
		}
		else {
			av_push( rv, &PL_sv_undef );
		}
	}
	if (unlikely(fcnt != 0)) {
		HV *used = (HV*)sv_2mortal((SV*)newHV());
		for (k=0; k <= av_len( fields );k++) {
			f = av_fetch( fields,k,0 );
			fl = hv_fetch_ent(hf,*f,0,0);
			if (fl && SvOK( HeVAL(fl) )) {
				(void) hv_store(used,SvPV_nolen(*f),sv_len(*f), &PL_sv_undef,0);
			}
		}
		if ((f = hv_fetch(hf,"",0,0)) && SvROK(*f)) {
			(void) hv_store(used,"",0, &PL_sv_undef,0);
		}
		(void) hv_iterinit( hf );
		STRLEN nlen;
		while ((fl = hv_iternext( hf ))) {
			char *name = HePV(fl, nlen);
			if (!hv_exists(used,name,nlen)) {
				warn("tuple key = %s; val = %s could not be used in hash fields",name, SvPV_nolen(HeVAL(fl)));
			}
		}
	}
	return rv;
}


static inline uint32_t decode_pkt_len(char **h) {
	char *p = *h;
	uint32_t l = *((uint32_t *)(p+1));
	*h += 5;
	return be32toh(l);
}

static inline void write_length(char *h, uint32_t size) {
	*h = 0xce;
	*((uint32_t *)(h+1)) = htobe32(size);
}

static inline char * write_iid(char *h, uint32_t iid) {
	*h = 0xce;
	*(uint32_t*)(h + 1) = htobe32(iid);
	h += 5;
	return h;
}

static inline SV * pkt_ping( uint32_t iid ) {
	int sz = 5 +
		mp_sizeof_map(2) +
		mp_sizeof_uint(TP_CODE) +
		mp_sizeof_uint(TP_PING) +
		mp_sizeof_uint(TP_SYNC) +
		5;

	SV * rv = newSV(sz);
	SvUPGRADE(rv, SVt_PV);
	SvPOK_on(rv);

	char *h = (char *) SvPVX(rv);

	write_length(h, sz-5);
	h = mp_encode_map(h + 5, 2);
	h = mp_encode_uint(h, TP_CODE);
	h = mp_encode_uint(h, TP_PING);
	h = mp_encode_uint(h, TP_SYNC);
	h = write_iid(h, iid);

	SvCUR_set(rv, sz);
	return rv;
}

static inline SV * pkt_select(TntCtx *ctx, uint32_t iid, HV * spaces, SV *space, SV *keys, HV * opt, SV *cb) {
	// register uniptr p;

	U32 limit  = 0xffffffff;
	U32 offset = -1;
	U32 index  = -1;
	U32 flags  = 0;

	unpack_format *fmt;
	dUnpackFormat( format );

	int k,i;
	SV **key;

	TntSpace *spc = 0;
	TntIndex *idx = 0;

	if(( spc = evt_find_space( space, spaces ) )) {
		ctx->space = spc;
	}
	else {
		ctx->use_hash = 0;
	}

	if (opt) {
		if ((key = hv_fetch(opt, "index", 5, 0)) && SvOK(*key)) {
			if(( idx = evt_find_index( spc, key ) ))
				index = idx->id;
		}
		if ((key = hv_fetchs(opt, "limit", 0)) && SvOK(*key)) limit = SvUV(*key);
		if ((key = hv_fetchs(opt, "offset", 0)) && SvOK(*key)) offset = SvUV(*key);

		// if ((key = hv_fetchs(opt, "quiet", 0)) && SvOK(*key)) flags |= TNT_FLAG_BOX_QUIET;
		// if ((key = hv_fetchs(opt, "nostore", 0)) && SvOK(*key)) flags |= TNT_FLAG_NOT_STORE;
		if ((key = hv_fetchs(opt, "hash", 0)) ) ctx->use_hash = SvOK(*key) ? SvIV( *key ) : 0;
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

	evt_opt_out( opt, ctx, spc );
	evt_opt_in( opt, ctx, idx );

	fmt = &ctx->f;

	// TODO: add ITERATOR

	int body_map_sz = 3 + (index != -1) + (offset != -1);
	int keys_size = 0;

	int sz = 5 +
		mp_sizeof_map(2) +
		mp_sizeof_uint(TP_CODE) +
		mp_sizeof_uint(TP_SELECT) +
		mp_sizeof_uint(TP_SYNC) +
		5 +
		mp_sizeof_map(body_map_sz) +
		mp_sizeof_uint(TP_SPACE) +
		mp_sizeof_uint(spc->id) +
		mp_sizeof_uint(TP_LIMIT) +
		mp_sizeof_uint(limit);

	if (index != -1) {
		sz += mp_sizeof_uint(TP_INDEX) +
			  mp_sizeof_uint(index);
	}

	if (offset != -1) {
		sz += mp_sizeof_uint(TP_OFFSET) +
			  mp_sizeof_uint(offset);
	}


	sz += mp_sizeof_uint(TP_KEY);


	// keys



	if (unlikely( !keys || !SvROK(keys) || ( (SvTYPE(SvRV(keys)) != SVt_PVAV) && (SvTYPE(SvRV(keys)) != SVt_PVHV) ) )) {
		if (!ctx->f.nofree) safefree(ctx->f.f);
		croak_cb(cb,"keys must be ARRAYREF or HASHREF");
	}

	SV *t = keys;

	AV *fields;
	if ((SvTYPE(SvRV(t)) == SVt_PVHV)) { fields = hash_to_array_fields( (HV *) SvRV(t), idx->fields, cb ); }
	else { fields  = (AV *) SvRV(t); }

	keys_size = av_len(fields) + 1;
	sz += mp_sizeof_array(keys_size);

	for (k = 0; k < keys_size; k++) {
		key = av_fetch( fields, k, 0 );
		if (key && *key) {
			if ( !SvOK(*key) || !sv_len(*key) ) {
				cwarn("something is going on wrong1");
			} else {
				sz += mp_sizeof_str(sv_len(*key));
			}
		}
		else {
			cwarn("something is going on wrong2");
		}
	}


	SV *rv = sv_2mortal(newSV( sz )); // to avoid reallocation we request + TUPLE_FIELD_DEFAULT*tuple_count because tuples probably will not be empty
	SvUPGRADE( rv, SVt_PV );
	SvPOK_on(rv);
	//warn("before: sv_len:%d, sv_pvx:%p, sv_cur:%d",SvLEN(rv), SvPVX(rv), SvCUR(rv));

	char *p = (char *) SvPVX(rv);
	char *h = p;
	h = mp_encode_map(p + 5, 2);
	h = mp_encode_uint(h, TP_CODE);
	h = mp_encode_uint(h, TP_SELECT);
	h = mp_encode_uint(h, TP_SYNC);
	h = write_iid(h, iid);
	// int count = 5;
	// if (limit == -1) {
	// 	--count;
	// }
	h = mp_encode_map(h, body_map_sz);
	h = mp_encode_uint(h, TP_SPACE);
	h = mp_encode_uint(h, spc->id);
	h = mp_encode_uint(h, TP_LIMIT);
	h = mp_encode_uint(h, limit);

	if (index != -1) {
		h = mp_encode_uint(h, TP_INDEX);
		h = mp_encode_uint(h, index);
	}
	// cwarn("%d", index);
	if (offset != -1) {
		h = mp_encode_uint(h, TP_OFFSET);
		h = mp_encode_uint(h, offset);
	}

	h = mp_encode_uint(h, TP_KEY);
	h = mp_encode_array(h, keys_size);



	for (k=0; k < keys_size; k++) {
		key = av_fetch( fields, k, 0 );
		if (key && *key) {
			if ( !SvOK(*key) || !sv_len(*key) ) {
				cwarn("something is going on wrong1");
				// var_len -= TUPLE_FIELD_DEFAULT;
				// *(p.c++) = 0;
			} else {
				// cwarn("data.key: %.*s", (int) sv_len(*key), SvPV_nolen(*key));
				// h = mp_encode_str(h, SvPV_nolen(*key), sv_len(*key));
				field_sv_fmt( h, *key, k < fmt->size ? fmt->f[k] : fmt->def );
				// switch(SvTYPE(SvRV(t))) {
				// case SVt_IV:
				// 	h = mp_encode_int(h, SvIV(*key));
				// 	break;
				// case SVt_PV:
				// 	h = mp_encode_str(h, SvPV_nolen(*key), sv_len(*key));
				// 	break;
				// }

				// var_len += sv_len(*key) - TUPLE_FIELD_DEFAULT;
				// uptr_sv_check( p, rv, const_len + var_len );
				// uptr_field_sv_fmt( p, *key, k < fmt->size ? fmt->f[k] : fmt->def );
			}
		}
		else {
			cwarn("something is going on wrong2");
			// var_len -= TUPLE_FIELD_DEFAULT;
			// *(p.c++) = 0;
		}
	}


	write_length(p, h-p-5);
	SvCUR_set(rv, h-p);



	// int tuple_count = ( av_len(keys)+1 );
	// STRLEN const_len, var_len;
	// const_len =
	// 	sizeof(tnt_pkt_select_t) // base of requset
	// 	+ 4 * tuple_count // (<cardinality>) * tuple_count
	// ;



	// for (i = 0; i <= av_len(keys); i++) {
	// 	key = av_fetch( keys, i, 0 );
	// 	if (unlikely( !key || !*key || !SvROK(*key) || ( (SvTYPE(SvRV(*key)) != SVt_PVAV) && (SvTYPE(SvRV(*key)) != SVt_PVHV) ) )) {
	// 		if (!ctx->f.nofree) safefree(ctx->f.f);
	// 		croak_cb(cb,"keys must be ARRAYREF of ARRAYREF or ARRAYREF of HASHREF");
	// 	}
	// 	SV *t = *key;
	// 	int cardinality = ( (SvTYPE(SvRV(t)) == SVt_PVHV) ? HvTOTALKEYS((HV*)SvRV(t)) : av_len((AV*)SvRV(t))+1 );
	// 	var_len += (5 + TUPLE_FIELD_DEFAULT) * cardinality;
	// 	//warn("(2)[%d]before c_len: %d and v_len: %d, sv_len:%d, sv_cur:%d",i, const_len, var_len, SvLEN(rv), p.c - SvPVX(rv));
	// 	uptr_tuple_calc_size( p, rv, t, idx->fields, fmt, const_len, var_len );
	// 	//warn("(2)[%d]after c_len: %d and v_len: %d, sv_len:%d, sv_cur:%d",i, const_len, var_len, SvLEN(rv), p.c - SvPVX(rv));
	// }

	// SvCUR_set( rv, p.c - SvPVX(rv) );

	// h = (tnt_pkt_select_t *) SvPVX( rv ); // for sure

	// h->type   = htole32( TNT_OP_SELECT );
	// h->reqid  = htole32( htole32( iid ) );
	// h->space  = htole32( htole32( spc->id ) );
	// h->index  = htole32( htole32( index ) );
	// h->offset = htole32( htole32( offset ) );
	// h->limit  = htole32( htole32( limit ) );
	// h->count  = htole32( htole32( av_len(keys) + 1 ) );
	// h->len    = htole32( SvCUR(rv) - sizeof( tnt_hdr_t ) );

	//warn("on return: sv_len:%d, sv_pvx:%p, sv_cur:%d",SvLEN(rv), SvPVX(rv), SvCUR(rv));
	return SvREFCNT_inc(rv);
}

static int parse_reply_hdr(HV *ret, const char const *data, STRLEN size, uint32_t *id) {
	const char *ptr, *beg, *end;

	const char *p = data;
	const char *test = p;

	// // len

	// if (mp_check(&test, data + size))
	// 	return -1;
	// if (mp_typeof(*p) != MP_UINT)
	// 	return -1;

	// uint32_t len = mp_decode_int(&p);

	// header
	test = p;
	if (mp_check(&test, data + size))
		return -1;
	if (mp_typeof(*p) != MP_MAP)
		return -1;

	uint32_t n = mp_decode_map(&p);
	uint32_t code;
	while (n-- > 0) {
		if (mp_typeof(*p) != MP_UINT)
			return -1;

		uint32_t key = mp_decode_uint(&p);
		switch (key) {
			case TP_CODE:
				if (mp_typeof(*p) != MP_UINT)
					return -1;

				code = mp_decode_uint(&p);
				cwarn("code: %d", code);
				break;

			case TP_SYNC:
				if (mp_typeof(*p) != MP_UINT)
					return -1;

				*id = mp_decode_uint(&p);
				break;
		}
	}

	// cwarn("code = %d", code);
	// cwarn("sync = %d", sync);

	(void) hv_stores(ret, "code", newSViv(code));
	(void) hv_stores(ret, "sync", newSViv(*id));

	return p - data;
}

static int parse_reply_body_data(HV *ret, const char const *data_begin, const char const *data_end, const unpack_format const * format, AV *fields) {
	STRLEN data_size = data_end - data_begin;
	if (data_size == 0)
		return 0;

	const char *p = data_begin;

	uint32_t cont_size = 0;
	switch (mp_typeof(*p)) {
	case MP_MAP:
		cont_size = mp_decode_map(&p);
		cwarn("map.size=%d", cont_size);
		// TODO: this is not valid probably
		break;

	case MP_ARRAY:
		cont_size = mp_decode_array(&p);
		cwarn("array.size=%d", cont_size);

		AV *tuples = newAV();
		av_extend(tuples, cont_size);
		(void) hv_stores(ret, "count", newSViv(cont_size));
		(void) hv_stores(ret, "tuples", newRV_noinc((SV *) tuples ));


		const char *str = NULL;
		uint32_t str_len = 0;
		uint32_t unum = 0;
		int32_t num = 0;

		uint32_t tuple_size = 0;
		uint32_t i = 0, k = 0;
		if (fields) { // using space definition
			SV **name;
			for (i = 0; i < cont_size; ++i) {
				HV* tuple = newHV();
				av_push(tuples, newRV_noinc((SV *)tuple));

				tuple_size = mp_decode_array(&p);

				for (k = 0; k < tuple_size; ++k) {
					name = av_fetch(fields, k, 0);
					if (name && *name) {
						switch (mp_typeof(*p)) {
						case MP_STR:
							str = mp_decode_str(&p, &str_len);
							// TODO: (void) hv_store(tuple, SvPV_nolen(*name), sv_len(*name), newSVpvn_pformat( ptr, fsize, format, k ), 0);
							(void) hv_store(tuple, SvPV_nolen(*name), sv_len(*name), newSVpvn(str, str_len), 0);
							// (void) av_push(tuple, newSVpvn(str, str_len));
							break;
						case MP_UINT:
							unum = mp_decode_uint(&p);
							(void) hv_store(tuple, SvPV_nolen(*name), sv_len(*name), newSVuv(unum), 0);
							// (void) av_push(tuple, newSVuv(unum));
							break;
						case MP_INT:
							num = mp_decode_int(&p);
							(void) hv_store(tuple, SvPV_nolen(*name), sv_len(*name), newSViv(num), 0);
							// (void) av_push(tuple, newSViv(num));
							break;
						}
					}
					else {
						cwarn("Field name for field %d is not defined",k);
					}
				}
			}

		} else {  //without space definition
			for (i = 0; i < cont_size; ++i) {
				AV* tuple = newAV();
				av_push(tuples, newRV_noinc((SV *)tuple));
				av_extend(tuple, tuple_size);

				tuple_size = mp_decode_array(&p);

				for (k = 0; k < tuple_size; ++k) {
					switch (mp_typeof(*p)) {
					case MP_STR:
						str = mp_decode_str(&p, &str_len);
						(void) av_push(tuple, newSVpvn(str, str_len));
						break;
					case MP_UINT:
						unum = mp_decode_uint(&p);
						(void) av_push(tuple, newSVuv(unum));
						break;
					case MP_INT:
						num = mp_decode_int(&p);
						(void) av_push(tuple, newSViv(num));
						break;
					}
				}
			}
		}

		break;
	}

	return 0;
}

static int parse_reply_body(HV *ret, const char const *data, STRLEN size, const unpack_format const * format, AV *fields) {
	const char *ptr, *beg, *end;

	const char *p = data;
	const char *test = p;
	// body
	if (p == data + size) {
		return size;
	}

	test = p;
	if (mp_check(&test, data + size))
		return -1;
	if (mp_typeof(*p) != MP_MAP)
		return -1;
	int n = mp_decode_map(&p);
	while (n-- > 0) {
		uint32_t key = mp_decode_uint(&p);
		switch (key) {
		case TP_ERROR: {
			if (mp_typeof(*p) != MP_STR)
				return -1;
			uint32_t elen = 0;
			const char *err_str = mp_decode_str(&p, &elen);

			(void) hv_stores(ret, "status", newSVpvs("error"));
			(void) hv_stores(ret, "errstr", newSVpvn(err_str, elen));
			break;
		}

		case TP_DATA: {
			if (mp_typeof(*p) != MP_ARRAY)
				return -1;

			(void) hv_stores(ret, "status", newSVpvs("ok"));
			const char *data_begin = p;
			mp_next(&p);
			parse_reply_body_data(ret, data_begin, p, format, fields);
			break;
		}
		}
		// r->bitmap |= (1ULL << key);
	}
	return p - data;
}
