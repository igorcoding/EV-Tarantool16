#include <string.h>

#include <endian.h>
#ifndef le64toh
# include <byteswap.h>
# if __BYTE_ORDER == __LITTLE_ENDIAN

#ifndef le16toh
#  define htobe16(x) __bswap_16 (x)
#  define htole16(x) (x)
#  define be16toh(x) __bswap_16 (x)
#  define le16toh(x) (x)
#endif

#ifndef le32toh
#  define htobe32(x) __bswap_32 (x)
#  define htole32(x) (x)
#  define be32toh(x) __bswap_32 (x)
#  define le32toh(x) (x)
#endif

#ifndef le64toh
#  define htobe64(x) __bswap_64 (x)
#  define htole64(x) (x)
#  define be64toh(x) __bswap_64 (x)
#  define le64toh(x) (x)
#endif

# else

#ifndef le16toh
#  define htobe16(x) (x)
#  define htole16(x) __bswap_16 (x)
#  define be16toh(x) (x)
#  define le16toh(x) __bswap_16 (x)
#endif

#ifndef le32toh
#  define htobe32(x) (x)
#  define htole32(x) __bswap_32 (x)
#  define be32toh(x) (x)
#  define le32toh(x) __bswap_32 (x)
#endif

#ifndef le64toh
#  define htobe64(x) (x)
#  define htole64(x) __bswap_64 (x)
#  define be64toh(x) (x)
#  define le64toh(x) __bswap_64 (x)
#endif
# endif
#endif

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


#ifndef I64
typedef int64_t I64;
#endif

#ifndef U64
typedef uint64_t U64;
#endif

#ifdef HAS_QUAD
#define HAS_LL 1
#else
#define HAS_LL 0
#endif



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
	char    def;
	char   *f;
	size_t size;
} unpack_format;


#define CHECK_PACK_FORMAT(src) \
	STMT_START { \
				char *p = src;\
				while(*p) { \
					switch(*p) { \
						case 'l':case 'L': \
							if (!HAS_LL) { croak("Int64 support was not compiled in"); break; } \
						case 'i':case 'I': \
						case 's':case 'S': \
						case 'c':case 'C': \
						case 'p':case 'u': \
							p++; break; \
						default: \
							croak("Unknown pattern in format: %c", *p); \
					} \
				} \
	} STMT_END

#define dUnpackFormat(fvar) unpack_format fvar; fvar.f = ""; fvar.size = 0; fvar.def  = 'p'

#define dExtractFormat(fvar,pos,usage) STMT_START {                  \
		if (items > pos) {                                           \
			if ( SvOK(ST(pos)) && SvPOK(ST(pos)) ) {                 \
				fvar.f = SvPVbyte(ST(pos), fvar.size);               \
				CHECK_PACK_FORMAT( fvar.f );                         \
			}                                                        \
			else if (!SvOK( ST(pos) )) {}                            \
			else {                                                   \
				croak("Usage: " usage " [ ,format_string [, default_unpack ] ] )");   \
			}                                                        \
			if ( items > pos + 1 && SvPOK(ST( pos + 1 )) ) {         \
				STRLEN _l;                                           \
				char * _p = SvPV( ST( pos + 1 ), _l );               \
				if (_l == 1 && ( *_p == 'p' || *_p == 'u' )) {       \
					format.def = *_p;                                \
				} else {                                             \
					croak("Bad default: %s; Usage: " usage " [ ,format_string [, default_unpack ] ] )", _p);   \
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
				croak("Usage ...");   \
			}                                                        \
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
				croak("Unsupported format: %s",format);    \
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
				croak("Unsupported format: %s",format);    \
		}                                                  \
	} STMT_END


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
							croak("Unsupported format: %s",format->f[ idx ]);
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

static int parse_reply(HV *ret, const char const *data, STRLEN size, const unpack_format const * format) {
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
		//warn("small header");
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
		//warn("Header ok but wrong len");
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
				//warn("error (%d)", end - data - 1);
				(void) hv_stores(ret, "status", newSVpvs("error"));
				(void) hv_stores(ret, "errstr", newSVpvn( data, end > data ? end - data - 1 : 0 ));
				data = end;
				break;
			} else {
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
				
				
				AV *tuple = newAV();
				if (cardinality < 1024) {
					av_extend(tuple, cardinality);
				}
				av_push(tuples, newRV_noinc((SV *)tuple));
				
				//warn("tuple[%d] with cardinality %d", i,cardinality);
				ptr = data;
				data += tsize;
				size -= tsize;
				
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
				};
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
		if ( up.c - SvPVX(svx) + need < SvLEN(svx) ) {} \
		else {\
			STRLEN used = up.c - SvPVX(svx); \
			up.c = sv_grow(svx, SvLEN(svx) + need ); \
			up.c += used; \
		}\
	} STMT_END
