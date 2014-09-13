#include <string.h>
#include "xsmy.h"

#define TNT_OP_INSERT      13
#define TNT_OP_SELECT      17
#define TNT_OP_UPDATE      19
#define TNT_OP_DELETE      21
#define TNT_OP_CALL        22
#define TNT_OP_PING        65280

#define TNT_FLAG_RETURN    0x01
#define TNT_FLAG_ADD       0x02
#define TNT_FLAG_REPLACE   0x04
#define TNT_FLAG_BOX_QUIET 0x08
#define TNT_FLAG_NOT_STORE 0x10

#define TUPLE_FIELD_DEFAULT 32
enum {
	TNT_UPDATE_ASSIGN = 0,
	TNT_UPDATE_ADD,
	TNT_UPDATE_AND,
	TNT_UPDATE_XOR,
	TNT_UPDATE_OR,
	TNT_UPDATE_SPLICE,
	TNT_UPDATE_DELETE,
	TNT_UPDATE_INSERT,
};

typedef struct {
	uint32_t type;
	uint32_t len;
	uint32_t reqid;
} tnt_hdr_t;

typedef struct {
	uint32_t type;
	uint32_t len;
	uint32_t reqid;
	uint32_t code;
} tnt_res_t;

typedef struct {
	uint32_t ns;
	uint32_t flags;
} tnt_hdr_nsf_t;

typedef struct {
	uint32_t type;
	uint32_t len;
	uint32_t reqid;
	uint32_t space;
	uint32_t flags;
} tnt_pkt_insert_t;

typedef tnt_pkt_insert_t tnt_pkt_delete_t;
typedef tnt_pkt_insert_t tnt_pkt_update_t;

typedef struct {
	uint32_t type;
	uint32_t len;
	uint32_t reqid;
	uint32_t space;
	uint32_t index;
	uint32_t offset;
	uint32_t limit;
	uint32_t count;
} tnt_pkt_select_t;


typedef struct {
	uint32_t type;
	uint32_t len;
	uint32_t reqid;
	uint32_t flags;
} tnt_pkt_call_t;


typedef
	union {
		char     *c;
		U32      *i;

		U64      *q;
		U16      *s;
	} uniptr;

unsigned char allowed_format[256] = {
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 
	0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 
	1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};


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

#define dUnpackFormat(fvar) unpack_format fvar; fvar.f = ""; fvar.nofree = 1; fvar.size = 0; fvar.def  = 'p'

#define dExtractFormat(fvar,pos,usage) STMT_START {                  \
		if (items > pos) {                                           \
			if ( SvOK(ST(pos)) && SvPOK(ST(pos)) ) {                 \
				fvar.f = SvPVbyte(ST(pos), fvar.size);               \
				CHECK_PACK_FORMAT( fvar.f );                         \
			}                                                        \
			else if (!SvOK( ST(pos) )) {}                            \
			else {                                                   \
				croak_cb(cb,"Usage: " usage " [ ,format_string [, default_unpack ] ] )");   \
			}                                                        \
			if ( items > pos + 1 && SvPOK(ST( pos + 1 )) ) {         \
				STRLEN _l;                                           \
				char * _p = SvPV( ST( pos + 1 ), _l );               \
				if (_l == 1 && ( *_p == 'p' || *_p == 'u' )) {       \
					format.def = *_p;                                \
				} else {                                             \
					croak_cb(cb,"Bad default: %s; Usage: " usage " [ ,format_string [, default_unpack ] ] )", _p);   \
				}                                                    \
			}                                                        \
			                                                         \
		}                                                            \
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

#define uptr_cat_sv_fmt( up, src, format )                                 \
	STMT_START {                                                           \
		switch( format ) {                                                 \
			case 'l': *( up.q ++ ) = htole64( (U64) SvIV( src ) ); break; \
			case 'L': *( up.q ++ ) = htole64( (U64) SvUV( src ) ); break; \
			case 'i': *( up.i ++ ) = htole32( (U32) SvIV( src ) ); break; \
			case 'I': *( up.i ++ ) = htole32( (U32) SvUV( src ) ); break; \
			case 's': *( up.s ++ ) = htole16( (U16) SvIV( src ) ); break; \
			case 'S': *( up.s ++ ) = htole16( (U16) SvUV( src ) ); break; \
			case 'c': *( up.c ++ ) = (U8) SvIV( src ); break;             \
			case 'C': *( up.c ++ ) = (U8) SvUV( src ); break;             \
			case 'p': case 'u':                                           \
				memcpy( up.c, SvPV_nolen(src), sv_len(src)  );        \
				up.c += sv_len(src);                                      \
				break;                                                    \
			default:                                       \
				croak_cb(cb,"Unsupported format: %c",format);    \
		}                                                  \
	} STMT_END

#define uptr_field_sv_fmt( up, src, format )                               \
	STMT_START {                                                           \
		switch( format ) {                                                 \
			case 'l': *(up.c++) = 8; *( up.q++ ) = htole64( (U64) SvIV( src ) ); break; \
			case 'L': *(up.c++) = 8; *( up.q++ ) = htole64( (U64) SvUV( src ) ); break; \
			case 'i': *(up.c++) = 4; *( up.i++ ) = htole32( (U32) SvIV( src ) ); break; \
			case 'I': *(up.c++) = 4; *( up.i++ ) = htole32( (U32) SvUV( src ) ); break; \
			case 's': *(up.c++) = 2; *( up.s++ ) = htole16( (U16) SvIV( src ) ); break; \
			case 'S': *(up.c++) = 2; *( up.s++ ) = htole16( (U16) SvUV( src ) ); break; \
			case 'c': *(up.c++) = 1; *( up.c++ ) = (U8) SvIV( src ); break;             \
			case 'C': *(up.c++) = 1; *( up.c++ ) = (U8) SvUV( src ); break;             \
			case 'p': case 'u':                                           \
				up.c = varint( up.c, sv_len(src) );                        \
				memcpy( up.c, SvPV_nolen(src), sv_len(src)  );         \
				up.c += sv_len(src);                                       \
				break;                                                    \
			default:                                       \
				croak_cb(cb,"Unsupported format: %c",format);    \
		}                                                  \
	} STMT_END

#define TNT_CALL_PREALLOC_SIZE(proclen,ntuples) \
				( ( ( ( \
					sizeof( tnt_pkt_call_t ) + 4 \
					+ ( proclen) \
					+ ( ntuples ) * ( 5 + 32 ) \
					+ 16 \
				) >> 5 ) << 5 ) + 0x20 )

#define TNT_SELECT_PREALLOC_SIZE(ntuples) \
				( ( ( ( \
					sizeof( tnt_pkt_select_t ) + 4 \
					+ ( ntuples ) * ( 5 + 32 ) \
					+ 16 \
				) >> 5 ) << 5 ) + 0x20 )
#define TNT_INSERT_PREALLOC_SIZE(ntuples) \
				( ( ( ( \
					sizeof( tnt_pkt_insert_t ) + 4 \
					+ ( ntuples ) * ( 5 + 32 ) \
					+ 16 \
				) >> 5 ) << 5 ) + 0x20 )

#define TNT_UPDATE_PREALLOC_SIZE(ntuples, nops) \
				( ( ( ( \
					sizeof( tnt_pkt_update_t ) + 8 \
					+ ( ntuples ) * ( 5 + 32 ) \
					+ ( nops ) * ( 4 + 1 + 5 + 32 ) \
					+ 128 \
				) >> 5 ) << 5 ) + 0x20 )

#define uptr_tuple(p, rv, t, hfields, fmt) STMT_START { \
	AV *fields; \
	if ((SvTYPE(SvRV(t)) == SVt_PVHV)) { fields = hash_to_array_fields( (HV *) SvRV(t), hfields, cb ); } \
	else { fields  = (AV *) SvRV(t); } \
	*( p.i++ ) = htole32( av_len(fields) + 1 ); \
	for (k=0; k <= av_len(fields); k++) { \
		key = av_fetch( fields, k, 0 ); \
		if (key && *key) { \
			if ( !SvOK(*key) || !sv_len(*key) ) { *(p.c++) = 0; } \
			else { \
				uptr_sv_size( p, rv, 5 + 8 + sv_len(*key) ); \
				uptr_field_sv_fmt( p, *key, k < fmt->size ? fmt->f[k] : fmt->def ); \
			} \
		} \
		else {\
			uptr_sv_size( p, rv, 1 ); \
			*(p.c++) = 0;\
		} \
	} \
} STMT_END

#define uptr_tuple_calc_size(p, rv, t, hfields, fmt, const_len, var_len) STMT_START { \
	AV *fields; \
	if ((SvTYPE(SvRV(t)) == SVt_PVHV)) { fields = hash_to_array_fields( (HV *) SvRV(t), hfields, cb ); } \
	else { fields  = (AV *) SvRV(t); } \
	*( p.i++ ) = htole32( av_len(fields) + 1 ); \
	for (k=0; k <= av_len(fields); k++) { \
		key = av_fetch( fields, k, 0 ); \
		if (key && *key) { \
			if ( !SvOK(*key) || !sv_len(*key) ) { \
				var_len -= TUPLE_FIELD_DEFAULT; \
				*(p.c++) = 0; \
			} else { \
				var_len += sv_len(*key) - TUPLE_FIELD_DEFAULT; \
				uptr_sv_check( p, rv, const_len + var_len ); \
				uptr_field_sv_fmt( p, *key, k < fmt->size ? fmt->f[k] : fmt->def ); \
			} \
		} \
		else {\
			var_len -= TUPLE_FIELD_DEFAULT; \
			*(p.c++) = 0;\
		} \
	} \
} STMT_END

#define check_tuple(tuple, allow_hash) STMT_START { \
	if (SvROK(tuple)) { \
		if ( SvTYPE(SvRV(tuple)) == SVt_PVHV ) { \
			if (unlikely(!(allow_hash))) { \
				croak_cb(cb,"Cannot use hash without space or index"); \
			} \
		} else \
		if ( unlikely(SvTYPE(SvRV(tuple)) != SVt_PVAV) ) { \
			croak_cb(cb,"Tuple must be %s, but have %s", (allow_hash) ? "ARRAYREF or HASHREF" : "ARRAYREF", SvPV_nolen(tuple) ); \
		} \
	} else { \
		croak_cb(cb,"Tuple must be %s, but have %s", (allow_hash) ? "ARRAYREF or HASHREF" : "ARRAYREF", SvPV_nolen(tuple) ); \
	} \
} STMT_END

//DUMP//////////////////////
char *  dumper(SV *any) {
	dSP;

	ENTER;
	SAVETMPS;

	PUSHMARK(SP);
	EXTEND(SP,1);
	PUSHs( any );
	PUTBACK;

	int count = call_pv( "Data::Dumper::Dumper", G_SCALAR | G_EVAL | G_KEEPERR );

	SPAGAIN;
	if (count != 1) croak("XXX");
	if (SvTRUE(ERRSV)) {
			croak("Error - %s\n", SvPV_nolen(ERRSV));
	}
	SV *ret = POPs;
	SvREFCNT_inc(ret);
	//warn("Dump = %s\n",SvPV_nolen(ret));
	//printf("Dump = %s\n",POPp);

	PUTBACK;

	FREETMPS;
	LEAVE;

	return SvPV_nolen(ret);
}
////////////////////////////

static inline SV * newSVpvn_pformat ( const char *data, STRLEN size, const unpack_format * format, int idx ) {
	assert(size >= 0);
			if (format && idx < format->size) {
					switch( format->f[ idx ] ) {
						case 'l':
							if (size != 8) warn("Field l should be of size 8, but got: %u", (uint) size);
							return newSViv( le64toh( *( I64 *) data ) );
							break;
						case 'L':
							if (size != 8) warn("Field L should be of size 8, but got: %u", (uint) size);
							return newSVuv( le64toh( *( U64 *) data ) );
							break;
						case 'i':
							if (size != 4) warn("Field i should be of size 4, but got: %u", (uint) size);
							return newSViv( le32toh( *( I32 *) data ) );
							break;
						case 'I':
							if (size != 4) warn("Field I should be of size 4, but got: %u", (uint) size);
							return newSVuv( le32toh( *( U32 *) data ) );
							break;
						case 's':
							if (size != 2) warn("Field s should be of size 2, but got: %u", (uint) size);
							return newSViv( le16toh( *( I16 *) data ) );
							break;
						case 'S':
							if (size != 2) warn("Field S should be of size 2, but got: %u", (uint) size);
							return newSVuv( le16toh( *( U16 *) data ) );
							break;
						case 'c':
							if (size != 1) warn("Field c should be of size 1, but got: %u", (uint) size);
							return newSViv( *( I8 *) data );
							break;
						case 'C':
							if (size != 1) warn("Field C should be of size 1, but got: %u", (uint) size);
							return newSVuv( *( U8 *) data );
							break;
						case 'p':
							return newSVpvn_utf8(data, size, 0);
							break;
						case 'u':
							return newSVpvn_utf8(data, size, 1);
							break;
						default:
							warn("Unsupported format: %c",format->f[ idx ]);
							return newSVpvn_utf8(data, size, 0);
					}
			} else { // no format
				if (format->def == 'u') {
					return newSVpvn_utf8(data, size, 1);
				} else {
					return newSVpvn_utf8(data, size, 0);
				}
			}
}

/*
	should return size of the packet captured.
	return 0 on short read
	return -1 on fatal error
*/

static int parse_reply(HV *ret, const char const *data, STRLEN size, const unpack_format const * format, AV *fields) {
	const char *ptr, *beg, *end;
	
	//warn("parse data of size %d",size);
	if ( size < sizeof(tnt_res_t) ) { // ping could have no count, so + 4
		if ( size >= sizeof(tnt_hdr_t) ) {
			tnt_hdr_t *hx = (tnt_hdr_t *) data;
			//warn ("rcv at least hdr: %d/%d", le32toh( hx->type ), le32toh( hx->len ));
			if ( le32toh( hx->type ) == TNT_OP_PING && le32toh( hx->len ) == 0 ) {
				(void) hv_stores(ret, "code", newSViv( 0 ));
				(void) hv_stores(ret, "status", newSVpvs("ok"));
				(void) hv_stores(ret, "id",   newSViv( le32toh( hx->reqid ) ));
				(void) hv_stores(ret, "type", newSViv( le32toh( hx->type ) ));
				return sizeof(tnt_hdr_t);
			} else {
				//warn("not a ping<%u> or wrong len<%u>!=0 for size=%u", le32toh( hx->type ), le32toh( hx->len ), size);
			}
		}
		warn("small header");
		goto shortread;
	}
	
	beg = data; // save ptr;
	
	tnt_res_t *hd = (tnt_res_t *) data;
	
	uint32_t type = le32toh( hd->type );
	uint32_t len  = le32toh( hd->len );
	uint32_t code = le32toh( hd->code );
	
	(void) hv_stores(ret, "type", newSViv( type ));
	(void) hv_stores(ret, "code", newSViv( code ));
	(void) hv_stores(ret, "id",   newSViv( le32toh( hd->reqid ) ));
	
	if ( size < len + sizeof(tnt_res_t) - 4 ) {
		warn("Header ok but wrong len (size=%zd < len = %u)", size, len);
		goto shortread;
	}
	
	data += sizeof(tnt_res_t);
	end = data + len - 4;
	
	
	//warn ("type = %d, len=%d (size=%d/%d)", type, len, size, size - sizeof( tnt_hdr_t ));
	switch (type) {
		case TNT_OP_PING:
			return data - beg;
		case TNT_OP_UPDATE:
		case TNT_OP_INSERT:
		case TNT_OP_DELETE:
		case TNT_OP_SELECT:
		case TNT_OP_CALL:
			
			if (code != 0) {
				//warn("reqid:%d; error (len:%d); typ=%d; code=%d; len=%d; %-.*s",hd->reqid, end - data - 1, type, code, len, end > data ? end - data - 1 : 0, data);
				(void) hv_stores(ret, "status", newSVpvs("error"));
				(void) hv_stores(ret, "errstr", newSVpvn( data, end > data ? end - data - 1 : 0 ));
				data = end;
				break;
			} else {
				//warn("reqid:%d; (len:%d); typ=%d; code=%d; len=%d;",hd->reqid, end - data - 1, type, code, len);
				(void) hv_stores(ret, "status", newSVpvs("ok"));
			}
			
			if (data == end) {
				// result without tuples
				//warn("no more data");
				break;
			}
			/*
			if ( len == 0 ) {
				// no tuple data to read.
				//warn("h.len == 0");
				break;
			} else {
				//warn("have more len: %d", len);
			}
			*/
			
			uint32_t count = le32toh( ( *(uint32_t *) data ) );
			//warn ("count = %d",count);
			
			data += 4;
			
			(void) hv_stores(ret, "count", newSViv(count));
			
			if (data == end) {
				// result without tuples
				//warn("no more data");
				break;
			} else {
				//warn("have more data: +%u", end - data);
			}
			
			if (data > end) {
				//warn("data > end");
				data = end;
				break;
			}
			
			int i,k;
			AV *tuples = newAV();
			//warn("count = %d", count);
			if (count < 1024) {
				av_extend(tuples, count);
			}
			
			(void) hv_stores( ret, "tuples", newRV_noinc( (SV *) tuples ) );
			for (i=0;i < count;i++) {
				uint32_t tsize = le32toh( ( *(uint32_t *) data ) ); data += 4;
				//warn("tuple %d size = %u",i,tsize);
				if (data + tsize > end) {
					warn("Intersection1: data=%p, size = %u, end = %p", data, tsize, end);
					goto intersection;
				}
					
				uint32_t cardinality = le32toh( ( *(uint32_t *) data ) ); data +=4;
				
				ptr = data;
				data += tsize;
				size -= tsize;
				if (fields) {
					HV *tuple = newHV();
					AV *tail = 0;
					SV **name;
					unsigned last = av_len(fields);
					av_push(tuples, newRV_noinc((SV *)tuple));
					for ( k=0; k < cardinality; k++ ) {
						unsigned int fsize = 0;
						do { fsize = ( fsize << 7 ) | ( *ptr & 0x7f ); } while ( *ptr++ & 0x80 && ptr < end );
						
						if (ptr + fsize > end) {
							warn("Intersection2: k=%d < card=%d (fsize: %d) (ptr: %p :: end: %p)", k, cardinality, fsize, ptr, end);
							goto intersection;
						}
						if ( k <= last ) {
							name = av_fetch(fields, k, 0);
							if (name && *name) {
								(void) hv_store(tuple,SvPV_nolen(*name),sv_len(*name),newSVpvn_pformat( ptr, fsize, format, k ), 0);
							}
							else {
								cwarn("Field name for field %d is not defined",k);
							}
						}
						else  {
							if ( !tail ) {
								tail = newAV();
								(void) hv_stores(tuple,"",newRV_noinc( (SV *) tail ));
							}
							av_push( tail, newSVpvn_pformat( ptr, fsize, format, k ) );
						}
						ptr += fsize;
					}
					
				} else {
					AV *tuple = newAV();
					if (cardinality < 1024) {
						av_extend(tuple, cardinality);
					}
					av_push(tuples, newRV_noinc((SV *)tuple));
					
					for ( k=0; k < cardinality; k++ ) {
						unsigned int fsize = 0;
						do {
							fsize = ( fsize << 7 ) | ( *ptr & 0x7f );
						} while ( *ptr++ & 0x80 && ptr < end );
						
						if (ptr + fsize > end) {
							warn("Intersection2: k=%d < card=%d (fsize: %d) (ptr: %p :: end: %p)", k, cardinality, fsize, ptr, end);
							goto intersection;
						}
						
						av_push( tuple, newSVpvn_pformat( ptr, fsize, format, k ) );
						ptr += fsize;
					}
				}
			}
			break;
		default:
			(void) hv_stores(ret, "status", newSVpvs("type"));
			(void) hv_stores(ret, "errstr", newSVpvf("Unknown type of operation: 0x%04x", type));
			return end - beg;
	}
	return end - beg;
	
	intersection:
		(void) hv_stores(ret, "status", newSVpvs("intersect"));
		(void) hv_stores(ret, "errstr", newSVpvs("Nested structure intersect packet boundary"));
		return end - beg;
	shortread:
		(void) hv_stores(ret, "status", newSVpvs("buffer"));
		(void) hv_stores(ret, "errstr", newSVpvs("Input data too short"));
		return 0;
}


static inline ptrdiff_t varint_write(char *buf, uint32_t value) {
	char *begin = buf;
	if ( value >= (1 << 7) ) {
		if ( value >= (1 << 14) ) {
			if ( value >= (1 << 21) ) {
				if ( value >= (1 << 28) ) {
					*(buf++) = (value >> 28) | 0x80;
				}
				*(buf++) = (value >> 21) | 0x80;
			}
			*(buf++) = ((value >> 14) | 0x80);
		}
		*(buf++) = ((value >> 7) | 0x80);
	}
	*(buf++) = ((value) & 0x7F);
	return buf - begin;
}

static inline char * varint(char *buf, uint32_t value) {
	if ( value >= (1 << 7) ) {
		if ( value >= (1 << 14) ) {
			if ( value >= (1 << 21) ) {
				if ( value >= (1 << 28) ) {
					*(buf++) = (value >> 28) | 0x80;
				}
				*(buf++) = (value >> 21) | 0x80;
			}
			*(buf++) = ((value >> 14) | 0x80);
		}
		*(buf++) = ((value >> 7) | 0x80);
	}
	*(buf++) = ((value) & 0x7F);
	return buf;
}

int varint_size(uint32_t value) {
	if (value < (1 << 7 )) return 1;
	if (value < (1 << 14)) return 2;
	if (value < (1 << 21)) return 3;
	if (value < (1 << 28)) return 4;
	                       return 5;
}


#define uptr_sv_size( up, svx, need ) \
	STMT_START {                                                           \
		if ( up.c - SvPVX(svx) + need < SvLEN(svx) )  { \
		} \
		else {\
			STRLEN used = up.c - SvPVX(svx); \
			up.c = sv_grow(svx, SvLEN(svx) + need ); \
			up.c += used; \
		}\
	} STMT_END

#define uptr_sv_check( up, svx, totalneed ) \
	STMT_START {                                                           \
		if ( totalneed < SvLEN(svx) )  { \
		} \
		else {\
			STRLEN used = up.c - SvPVX(svx); \
			up.c = sv_grow(svx, totalneed ); \
			up.c += used; \
		}\
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

static inline SV * pkt_ping( uint32_t iid ) {
	SV * rv = newSV(12);
	SvUPGRADE( rv, SVt_PV );
	
	tnt_hdr_t *s = (tnt_hdr_t *) SvPVX( rv );
	s->type  = htole32( TNT_OP_PING );
	s->reqid = htole32( iid );
	s->len   = 0;
	
	return rv;
}


static inline SV * pkt_lua( TntCtx *ctx, uint32_t iid, HV * spaces, SV *proc, AV *tuple, HV * opt, SV * cb ) {
	register uniptr p;
	U32 flags = 0;
	SV **key;
	SV *rv;
	ctx->use_hash = 0; // default for lua is no hash
	
	if (opt) {
		if ((key = hv_fetchs(opt, "quiet", 0)) && SvOK(*key)) flags |= TNT_FLAG_BOX_QUIET;
		if ((key = hv_fetchs(opt, "nostore", 0)) && SvOK(*key)) flags |= TNT_FLAG_NOT_STORE;
		if ((key = hv_fetchs(opt,"space",0)) && SvOK(*key)) {
			if (( ctx->space = evt_find_space( *key, spaces ) )) {
				ctx->use_hash = 1;
			}
			else {
				//ctx->use_hash = 0;
			}
		}
		if ( (key = hv_fetchs(opt, "hash", 0)) ) {
			ctx->use_hash = SvOK(*key) ? SvIV( *key ) : 0;
			if (ctx->use_hash && ! ctx->space) {
				croak_cb(cb, "Use hash allowed only when have space");
			}
		}
	}
	
	unpack_format *fmt;
	dUnpackFormat( format );
	
	evt_opt_out( opt, ctx, ctx->space );
	evt_opt_in( opt, ctx, ((TntSpace *) 0) );
	
	//printf("tuple ");
	//dumper(sv_2mortal(newRV_inc((SV*) tuple)));

	int cardinality = ( av_len(tuple)+1 );
	STRLEN const_len, var_len;
	const_len =
		sizeof(tnt_pkt_call_t) // base of requset
		+ 5 + sv_len(proc) // proc_name: <w><data>
		+ 4 // cardinality
		+ 5 * cardinality // (field <w> ) * cardinality
	;
	var_len =
		TUPLE_FIELD_DEFAULT * cardinality // take 32 as average/estimated tuple field length
	;
	
	rv = sv_2mortal(newSV( const_len +  var_len ));
	SvUPGRADE( rv, SVt_PV );
	
	SvPOK_on(rv);
	
	tnt_pkt_call_t *h = (tnt_pkt_call_t *) SvPVX(rv);
	
	p.c = (char *)(h+1);
	
	uptr_field_sv_fmt( p, proc, 'p' );
	
	*(p.i++) = htole32( av_len(tuple) + 1 );
	
	int k;
	for (k=0; k <= av_len(tuple); k++) {
		SV *f = *av_fetch( tuple, k, 0 );
		if ( !SvOK(f) || !sv_len(f) ) {
			*(p.c++) = 0;
		} else {
			var_len += sv_len(f) - TUPLE_FIELD_DEFAULT;
			uptr_sv_check( p, rv, const_len + var_len );
			uptr_field_sv_fmt( p, f, k < format.size ? format.f[k] : format.def );
		}
	}

	SvCUR_set( rv, p.c - SvPVX(rv) );
	
	h = (tnt_pkt_call_t *) SvPVX( rv ); // for sure
	h->type   = htole32( TNT_OP_CALL );
	h->reqid  = htole32( iid );
	h->flags  = htole32( flags );
	h->len    = htole32( SvCUR(rv) - sizeof( tnt_hdr_t ) );
	//warn("reqid:%zd rv is_utf8=%zd and len=%d and cur=%d and LEN=%d and h->len=%d ",h->reqid, SvUTF8(rv),sv_len(rv),SvCUR(rv),SvLEN(rv),h->len);
	
	return SvREFCNT_inc(rv);
}

static inline SV * pkt_select( TntCtx *ctx, uint32_t iid, HV * spaces, SV *space, AV *keys, HV * opt, SV *cb ) {
	register uniptr p;
		
	U32 limit  = 0xffffffff;
	U32 offset = 0;
	U32 index  = 0;
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
		
		if ((key = hv_fetchs(opt, "quiet", 0)) && SvOK(*key)) flags |= TNT_FLAG_BOX_QUIET;
		if ((key = hv_fetchs(opt, "nostore", 0)) && SvOK(*key)) flags |= TNT_FLAG_NOT_STORE;
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

	int tuple_count = ( av_len(keys)+1 );
	STRLEN const_len, var_len;
	const_len =
		sizeof(tnt_pkt_select_t) // base of requset
		+ 4 * tuple_count // (<cardinality>) * tuple_count
	;
	var_len =
		0 // empty on start
	;

	SV *rv = sv_2mortal(newSV( const_len +  var_len + TUPLE_FIELD_DEFAULT*tuple_count)); // to avoid reallocation we request + TUPLE_FIELD_DEFAULT*tuple_count because tuples probably will not be empty
	SvUPGRADE( rv, SVt_PV );
	//warn("before: sv_len:%d, sv_pvx:%p, sv_cur:%d",SvLEN(rv), SvPVX(rv), SvCUR(rv));
	
	tnt_pkt_select_t *h = (tnt_pkt_select_t *) SvPVX(rv);
	
	p.c = (char *)(h+1);
		
	for (i = 0; i <= av_len(keys); i++) {
		key = av_fetch( keys, i, 0 );
		if (unlikely( !key || !*key || !SvROK(*key) || ( (SvTYPE(SvRV(*key)) != SVt_PVAV) && (SvTYPE(SvRV(*key)) != SVt_PVHV) ) )) {
			if (!ctx->f.nofree) safefree(ctx->f.f);
			croak_cb(cb,"keys must be ARRAYREF of ARRAYREF or ARRAYREF of HASHREF");
		}
		SV *t = *key;
		int cardinality = ( (SvTYPE(SvRV(t)) == SVt_PVHV) ? HvTOTALKEYS((HV*)SvRV(t)) : av_len((AV*)SvRV(t))+1 );
		var_len += (5 + TUPLE_FIELD_DEFAULT) * cardinality;
		//warn("(2)[%d]before c_len: %d and v_len: %d, sv_len:%d, sv_cur:%d",i, const_len, var_len, SvLEN(rv), p.c - SvPVX(rv));
		uptr_tuple_calc_size( p, rv, t, idx->fields, fmt, const_len, var_len );
		//warn("(2)[%d]after c_len: %d and v_len: %d, sv_len:%d, sv_cur:%d",i, const_len, var_len, SvLEN(rv), p.c - SvPVX(rv));
	}
	
	SvCUR_set( rv, p.c - SvPVX(rv) );
	
	h = (tnt_pkt_select_t *) SvPVX( rv ); // for sure
	
	h->type   = htole32( TNT_OP_SELECT );
	h->reqid  = htole32( htole32( iid ) );
	h->space  = htole32( htole32( spc->id ) );
	h->index  = htole32( htole32( index ) );
	h->offset = htole32( htole32( offset ) );
	h->limit  = htole32( htole32( limit ) );
	h->count  = htole32( htole32( av_len(keys) + 1 ) );
	h->len    = htole32( SvCUR(rv) - sizeof( tnt_hdr_t ) );
	
	//warn("on return: sv_len:%d, sv_pvx:%p, sv_cur:%d",SvLEN(rv), SvPVX(rv), SvCUR(rv));
	return SvREFCNT_inc(rv);
}


static inline SV * pkt_insert( TntCtx *ctx, uint32_t iid, HV * spaces, SV *space, SV *t, uint32_t insert_or_delete, HV * opt, SV * cb ) {
	register uniptr p;
	U32 flags = 0;
	
	unpack_format *fmt;
	dUnpackFormat( format );
		
	int k;
	SV **key;
	
	TntSpace *spc = 0;
	TntIndex *idx = 0;
	
	if(( spc = evt_find_space( space, spaces ) )) {
		ctx->space = spc;
		SV * i0 = sv_2mortal(newSVuv(0));
		key = &i0;
		idx = evt_find_index( spc, key );
	}
	else {
		ctx->use_hash = 0;
	}
	
	if (opt) {
		if ((key = hv_fetchs(opt, "return", 0)) && SvOK(*key)) flags |= TNT_FLAG_RETURN;
		if ((key = hv_fetchs(opt, "ret", 0)) && SvOK(*key)) flags |= TNT_FLAG_RETURN;
		if ((key = hv_fetchs(opt, "add", 0)) && SvOK(*key)) flags |= TNT_FLAG_ADD;
		if ((key = hv_fetchs(opt, "replace", 0)) && SvOK(*key)) flags |= TNT_FLAG_REPLACE;
		if ((key = hv_fetchs(opt, "rep", 0)) && SvOK(*key)) flags |= TNT_FLAG_REPLACE;
		if ((key = hv_fetchs(opt, "quiet", 0)) && SvOK(*key)) flags |= TNT_FLAG_BOX_QUIET;
		if ((key = hv_fetchs(opt, "nostore", 0)) && SvOK(*key)) flags |= TNT_FLAG_NOT_STORE;
		if ((key = hv_fetchs(opt, "hash", 0)) ) ctx->use_hash = SvOK(*key) ? SvIV( *key ) : 0;
	}
//TODO::
// insert uses wrong format.
	evt_opt_out( opt, ctx, spc );
	if (insert_or_delete == TNT_OP_INSERT) {
		check_tuple(t,spc);
		evt_opt_in( opt, ctx, spc );
	} else { // DELETE
		check_tuple(t, idx);
		evt_opt_in( opt, ctx, idx );
	}

	int cardinality = (  (SvTYPE(SvRV(t)) == SVt_PVHV) ? HvTOTALKEYS((HV*)SvRV(t)) : av_len((AV*)SvRV(t))+1 );
	STRLEN const_len, var_len;
	const_len =
		sizeof(tnt_pkt_update_t) // base of requset
		+ 4 // cardinality
		+ 5 * cardinality // (field <w> ) * cardinality
	;
	var_len =
		TUPLE_FIELD_DEFAULT * cardinality // take 32 as average/estimated tuple field length
	;

	SV *rv = sv_2mortal(newSV( const_len + var_len ));
	SvUPGRADE( rv, SVt_PV );

	tnt_pkt_insert_t *h = (tnt_pkt_insert_t *) SvPVX(rv);
	p.c = (char *)(h+1);
	
	uptr_tuple_calc_size(p, rv, t, ( insert_or_delete == TNT_OP_INSERT ? spc->fields : idx->fields ), fmt, const_len, var_len);
	
	SvCUR_set( rv, p.c - SvPVX(rv) );
	h = (tnt_pkt_insert_t *) SvPVX( rv ); // for sure
	h->type   = htole32( insert_or_delete );
	h->reqid  = htole32( iid );
	h->space  = htole32( spc->id );
	h->flags  = htole32( flags );
	h->len    = htole32( SvCUR(rv) - sizeof( tnt_hdr_t ) );
	return SvREFCNT_inc(rv);
}


static inline SV * pkt_update( TntCtx *ctx, uint32_t iid, HV * spaces, SV *space, SV *t, AV *ops, HV * opt, SV *cb ) {
	// in: tuple by index0. format from: index or in
	// out: tuple by spaceX. format from space or out
	register uniptr p;
	U32 flags = 0;
	
	unpack_format *fmt;
	dUnpackFormat( format );
	
	int k;
	SV **key,**val;
	
	
	TntSpace *spc = 0;
	TntIndex *idx = 0;
	
	if(( spc = evt_find_space( space, spaces ) )) {
		ctx->space = spc;
		SV * i0 = sv_2mortal(newSVuv(0));
		key = &i0;
		idx = evt_find_index( spc, key );
	}
	else {
		ctx->use_hash = 0;
	}
	
	check_tuple(t,idx);
	
	if (opt) {
		if ((key = hv_fetchs(opt, "return", 0)) && SvOK(*key)) flags |= TNT_FLAG_RETURN;
		if ((key = hv_fetchs(opt, "ret", 0)) && SvOK(*key)) flags |= TNT_FLAG_RETURN;
		if ((key = hv_fetchs(opt, "nostore", 0)) && SvOK(*key)) flags |= TNT_FLAG_NOT_STORE;
		if ((key = hv_fetchs(opt, "hash", 0)) ) ctx->use_hash = SvOK(*key) ? SvIV( *key ) : 0;
	}
	
	evt_opt_out( opt, ctx, spc );
	evt_opt_in( opt, ctx, idx );
	
	int cardinality = (  (SvTYPE(SvRV(t)) == SVt_PVHV) ? HvTOTALKEYS((HV*)SvRV(t)) : av_len((AV*)SvRV(t))+1 );
	int opcount = ( av_len(ops)+1 );
	STRLEN const_len, var_len;
	const_len =
		sizeof(tnt_pkt_update_t) // base of requset
		+ 4 // cardinality
		+ 5 * cardinality // cardinality * (field <w> )
		+ 4 // count
		+ (
			4 + // fieldno
			1 + // opcode
			5   // field <w>
		) * opcount;
	var_len =
		TUPLE_FIELD_DEFAULT * cardinality + // take 32 as average/estimated tuple field length
		( TUPLE_FIELD_DEFAULT ) * opcount // every field value
	;
	
	SV *rv = sv_2mortal(newSV( const_len + var_len ));
	
	SvUPGRADE( rv, SVt_PV );
	tnt_pkt_update_t *h = (tnt_pkt_update_t *) SvPVX(rv);
	
	p.c = (char *)(h+1);
	
	uptr_tuple_calc_size(p, rv, t, idx->fields, fmt, const_len, var_len);
	
	AV *aop;
	
	*( p.i++ ) = htole32( av_len(ops) + 1 );
	
	for (k = 0; k <= av_len( ops ); k++) {
		val = av_fetch( ops, k, 0 );
		if (!*val || !SvROK( *val ) || SvTYPE( SvRV(*val) ) != SVt_PVAV )
			croak_cb(cb,"Single update operation byst be arrayref");
		aop = (AV *)SvRV(*val);
		
		if ( av_len( aop ) < 1 ) croak_cb(cb,"Too short operation argument list");
		
		key = av_fetch( aop, 0, 0 );
		char field_format = 0;
		if (SvIOK(*key) && SvIVX(*key) >= 0) {
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
				croak_cb(cb,"Unknown field name: '%s' in space %d",SvPV_nolen( *key ), spc->id);
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
					var_len += sv_len(*val) - TUPLE_FIELD_DEFAULT;
					uptr_sv_check( p, rv, const_len + var_len );
					uptr_field_sv_fmt( p, *val, av_len(aop) > 2 ? *SvPV_nolen( *av_fetch( aop, 3, 0 ) ) : field_format ? field_format : 'p' );
				} else {
					warn("undef in assign");
					var_len -= TUPLE_FIELD_DEFAULT;
					*( p.c++ ) = 0;
				}
				break;
			case '!': // insert
				//if ( av_len( aop ) < 2 ) croak("Too short operation argument list for %c. Need 3 or 4, have %d", *opname, av_len( aop ) );
				*( p.c++ ) = TNT_UPDATE_INSERT;
				val = av_fetch( aop, 2, 0 );
				if (val && *val && SvOK(*val)) {
					var_len += sv_len(*val) - TUPLE_FIELD_DEFAULT;
					uptr_sv_check( p, rv, const_len + var_len );
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
				
				var_len += ( 1 + 4 + 1 + 4 + 5 + sv_len(*val) ) - TUPLE_FIELD_DEFAULT;
				uptr_sv_check( p, rv, const_len + var_len );
				
				p.c = varint( p.c, 1+4 + 1+4  + varint_size( sv_len(*val) ) + sv_len(*val) );
				
				*(p.c++) = 4;
				*(p.i++) = (U32)SvIV( *av_fetch( aop, 2, 0 ) ); // offset
				*(p.c++) = 4;
				*(p.i++) = (U32)SvIV( *av_fetch( aop, 3, 0 ) ); // length
				
				uptr_field_sv_fmt( p, *val, 'p' ); // string
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
				croak_cb(cb,"Unknown operation: %c", *opname);
		}
		if (opcode) { // Arith ops
			if ( av_len( aop ) < 2 ) croak_cb(cb,"Too short operation argument list for %c", *opname);
			
			*( p.c++ ) = opcode;
			
			unsigned long long v = SvUV( *av_fetch( aop, 2, 0 ) );
			if (v > 0xffffffff) {
				*( p.c++ ) = 8;
				*( p.q++ ) = (U64) v;
				var_len += 8 - TUPLE_FIELD_DEFAULT;
			} else {
				*( p.c++ ) = 4;
				*( p.i++ ) = (U32) v;
				var_len += 4 - TUPLE_FIELD_DEFAULT;
			}
		}
	}
	SvCUR_set( rv, p.c - SvPVX(rv) );
	
	h = (tnt_pkt_insert_t *) SvPVX( rv ); // for sure
	
	h->type   = htole32( TNT_OP_UPDATE );
	h->reqid  = htole32( iid );
	h->space  = htole32( spc ? spc->id : 0 );
	h->flags  = htole32( flags );
	h->len    = htole32( SvCUR(rv) - sizeof( tnt_hdr_t ) );
	
	return SvREFCNT_inc(rv);
}
