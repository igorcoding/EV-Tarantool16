#define MP_SOURCE 1

#include <string.h>
#include "xsmy.h"
#include "msgpuck.h"
#include "defines.h"

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
	TP_USERNAME = 0x23,
	TP_EXPRESSION = 0x27
};

/* response body */
enum tp_response_key_t {
	TP_DATA = 0x30,
	TP_ERROR = 0x31
};

/* request types */
enum tp_request_type {
	TP_SELECT = 0x01,
	TP_INSERT = 0x02,
	TP_REPLACE = 0x03,
	TP_UPDATE = 0x04,
	TP_DELETE = 0x05,
	TP_CALL = 0x06,
	TP_AUTH = 0x07,
	TP_EVAL = 0x08,
	TP_PING = 0x40
};

static const uint32_t SCRAMBLE_SIZE = 20;
static const uint32_t HEADER_CONST_LEN = 5 + // pkt_len
										 1 + // mp_sizeof_map(2) +
										 1 + // mp_sizeof_uint(TP_CODE) +
										 1 + // mp_sizeof_uint(TP COMMAND) +
										 1 + // mp_sizeof_uint(TP_SYNC) +
										 5;  // sync len
static const uint32_t EST_FIELD_SIZE = 5;


typedef struct {
	size_t  size;
	char   *f;
	int     nofree;
	char    def;
} unpack_format;


typedef struct {
	U32   id;
	SV   *name;
	SV   *type;
	U32   unique;
	AV   *fields;
	unpack_format f;
} TntIndex;

typedef struct {
	U32   id;
	SV   *name;
	SV   *owner;
	SV   *engine;
	SV   *fields_count;
	SV   *flags;

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
	SV   *name;
} TntField;

typedef enum {
	OP_UPD_ARITHMETIC,
	OP_UPD_DELETE,
	OP_UPD_INSERT_ASSIGN,
	OP_UPD_SPLICE,
	OP_UPD_UNKNOWN
} update_op_type_t;

typedef enum {
	FMT_UNKNOWN = 0,
	FMT_NUM,
	FMT_STR,
	FMT_NUMBER,
	FMT_INT,
	_FMT_SIZE
} TNT_FORMAT_TYPE;

static HV *types_boolean_stash;
static SV *types_true, *types_false;

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

#define sv_size_check( svx, svx_end, totalneed ) \
	STMT_START {                                                           \
		if ( totalneed < SvLEN(svx) )  { \
		} \
		else {\
			STRLEN used = svx_end - SvPVX(svx); \
			svx_end = sv_grow(svx, totalneed); \
			svx_end += used; \
		}\
	} STMT_END

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

static TntSpace * evt_find_space(SV *space, HV *spaces) {
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

#define dExtractFormat2(fvar,src) STMT_START {										\
		if ( SvOK(src) && SvPOK(src) ) {											\
			fvar.f = SvPVbyte(src, fvar.size);										\
			CHECK_PACK_FORMAT( fvar.f );											\
		}																			\
		else if (!SvOK( src )) {}													\
		else {																		\
			croak_cb(cb,"Usage { .. in => 'fmtstring', out => 'fmtstring' .. }");   \
		}																			\
} STMT_END



#define dExtractFormatCopy(fvar,src) STMT_START {									\
		if ( SvOK(src) && SvPOK(src) ) {											\
			(fvar)->f = SvPVbyte(src, (fvar)->size);								\
			CHECK_PACK_FORMAT( (fvar)->f );											\
			(fvar)->f = safecpy((fvar)->f,(fvar)->size); 							\
			(fvar)->nofree = 0;														\
		}																			\
		else if (!SvOK( src )) {}													\
		else {																		\
			croak_cb(cb,"Usage { .. in => 'fmtstring', out => 'fmtstring' .. }");   \
		}																			\
} STMT_END

#define evt_opt_out(opt,ctx,spc) STMT_START { 				\
	if (opt && (key = hv_fetchs(opt,"out",0)) && *key) { 	\
		dExtractFormatCopy( &ctx->f, *key ); 				\
	}														\
	else													\
	if (spc) {												\
		memcpy(&ctx->f,&spc->f,sizeof(unpack_format));		\
	}														\
	else													\
	{														\
		ctx->f.size = 0;									\
	}														\
} STMT_END

#define evt_opt_in(opt,ctx,idx) STMT_START { \
	if (opt && (key = hv_fetchs(opt,"in",0)) && *key) { 	\
		dExtractFormat2( format, *key ); 					\
		fmt = &format; 										\
	} 														\
	else 													\
	if (idx) { 												\
		fmt = &idx->f; 										\
	} 														\
	else 													\
	{ 														\
		fmt = &format; 										\
	} 														\
} STMT_END

// TOOD: add type deduction to here as well
#define field_size_sv_fmt( s, format )                                     \
	STMT_START {                                                           \
		switch( format ) {                                                 \
			case 'l':                                                      \
			case 'L':                                                      \
			case 'i':                                                      \
			case 'I':                                                      \
			case 's':                                                      \
			case 'S':                                                      \
			case 'c':                                                      \
			case 'C':                                                      \
				s = 9; break;                                              \
			case 'p': case 'u':                                            \
				s = 5; break;                                              \
			default:                                                       \
				s = 5; break;											\
		}                                                                  \
	} STMT_END

#define field_sv_fmt( dest, src, format )									\
	STMT_START {															\
		char fmt_is_num = -1;												\
		switch( format ) {													\
			case 'l':														\
			case 'i':														\
			case 's':														\
			case 'c':														\
			case 'L':														\
			case 'I':														\
			case 'S':														\
			case 'C':														\
				fmt_is_num = 1;												\
				break;														\
			case 'p': case 'u':												\
				fmt_is_num = 0;												\
				break;	 													\
			default:														\
				break;\
		}																	\
		if (fmt_is_num == 0 && SvPOK(src)) {								\
			dest = mp_encode_str(dest, SvPV_nolen(src), sv_len(src));		\
		}																	\
		else if ((SvIOK(src) || SvPOK(src))) {								\
			if (SvUOK(src)) {												\
				dest = mp_encode_uint(dest, SvUV(src));						\
			} else {														\
				IV num = SvIV(src);											\
				if (num >= 0) {												\
					dest = mp_encode_uint(dest, num);						\
				} else {													\
					dest = mp_encode_int(dest, num);						\
				}															\
			}																\
		} else if (fmt_is_num == -1) {										\
			croak_cb(cb,"Unsupported type: %d", SvTYPE(src));				\
		}																	\
	} STMT_END


static AV * hash_to_array_fields(HV * hf, AV *fields, SV * cb) {
	AV *rv = (AV *) sv_2mortal((SV *)newAV());
	int fcnt = HvTOTALKEYS(hf);
	int k;

	SV **f;
	HE *fl;

	// cwarn("still ok. fields = %p", fields);

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
			// TODO: not sure that we should ignore it
			// av_push( rv, &PL_sv_undef );
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

static inline void write_length(char *h, uint32_t size) {
	*h = 0xce;
	*((uint32_t *)(h+1)) = htobe32(size);
}

#define write_iid(h, iid) STMT_START {  \
	*h = 0xce;							\
	*(uint32_t*)(h + 1) = htobe32(iid); \
	h += 5;								\
} STMT_END


#define create_buffer(NAME, P_NAME, sz, tp_operation, iid) 				\
	SV *NAME = sv_2mortal(newSV((sz)));									\
	SvUPGRADE(NAME, SVt_PV);											\
	SvPOK_on(NAME);														\
																		\
	char *P_NAME = (char *) SvPVX(NAME);								\
	write_length(P_NAME, (sz)-5);										\
	P_NAME = mp_encode_map(P_NAME + 5, 2);								\
	P_NAME = mp_encode_uint(P_NAME, TP_CODE);							\
	P_NAME = mp_encode_uint(P_NAME, (tp_operation));					\
	P_NAME = mp_encode_uint(P_NAME, TP_SYNC);							\
	write_iid(P_NAME, (iid));											\

#define encode_keys(h, sz, fields, keys_size, fmt, key) STMT_START {	\
	uint8_t field_max_size = 0;											\
	for (k = 0; k < keys_size; k++) {									\
		key = av_fetch( fields, k, 0 );									\
		if (key && *key && SvOK(*key) && sv_len(*key)) {				\
			int _fmt = k < fmt->size ? fmt->f[k] : fmt->def;			\
			field_size_sv_fmt(field_max_size, _fmt);					\
			sz += field_max_size - EST_FIELD_SIZE;						\
			sv_size_check(rv, h, sz);									\
			field_sv_fmt( h, *key, _fmt);								\
		} else {														\
			cwarn("something is going wrong");							\
		}																\
	}																	\
} STMT_END

#define COMP_STR(lhs, rhs, lhs_len, rhs_len, res) STMT_START { 				  \
	if ((lhs_len) == (rhs_len) && strncasecmp((lhs), (rhs), rhs_len) == 0) {  \
		return (res);														  \
	} 																		  \
} STMT_END


static inline U32 get_iterator(SV *iterator_str) {
	const char *str = SvPVX(iterator_str);
	U32 str_len = SvCUR(iterator_str);
	COMP_STR(str, "EQ", str_len, 2, 0);
	COMP_STR(str, "REQ", str_len, 3, 1);
	COMP_STR(str, "ALL", str_len, 3, 2);
	COMP_STR(str, "LT", str_len, 2, 3);
	COMP_STR(str, "LE", str_len, 2, 4);
	COMP_STR(str, "GE", str_len, 2, 5);
	COMP_STR(str, "GT", str_len, 2, 6);
	COMP_STR(str, "BITS_ALL_SET", str_len, 12, 7);
	COMP_STR(str, "BITS_ANY_SET", str_len, 12, 8);
	COMP_STR(str, "BITS_ALL_NOT_SET", str_len, 16, 9);
	COMP_STR(str, "OVERLAPS", str_len, 8, 10);
	COMP_STR(str, "NEIGHBOR", str_len, 8, 11);
	return -1;
}


static inline update_op_type_t get_update_op_type(const char *op_str, uint32_t len) {
	if (unlikely(len != 1)) {
		return OP_UPD_UNKNOWN;
	}

	char op = op_str[0];

	if (op == '+' || op == '-' || op == '&' || op == '^' || op == '|') {
		return OP_UPD_ARITHMETIC;
	}

	if (op == '#') {
		return OP_UPD_DELETE;
	}

	if (op == '!' || op == '=') {
		return OP_UPD_INSERT_ASSIGN;
	}

	if (op == ':') {
		return OP_UPD_SPLICE;
	}

	return OP_UPD_UNKNOWN;
}


// static inline encode_obj(char *dest, SV *src, format, cb) {
// 	switch( format ) {
// 			case 'l': dest = mp_encode_int  ( dest, (U64) SvIV( src ) );  break;
// 			case 'L': dest = mp_encode_uint ( dest, (U64) SvUV( src ) );  break;
// 			case 'i': dest = mp_encode_int  ( dest, (U32) SvIV( src ) );  break;
// 			case 'I': dest = mp_encode_uint ( dest, (U32) SvUV( src ) );  break;
// 			case 's': dest = mp_encode_int  ( dest, (U16) SvIV( src ) );  break;
// 			case 'S': dest = mp_encode_uint ( dest, (U16) SvUV( src ) );  break;
// 			case 'c': dest = mp_encode_int  ( dest, (U8)  SvIV( src ) );  break;
// 			case 'C': dest = mp_encode_uint ( dest, (U8)  SvUV( src ) );  break;
// 			case 'p': case 'u':
// 				dest = mp_encode_str(dest, SvPV_nolen(src), sv_len(src)); break;
// 			case 'a': {
// 				if (SvTYPE(SvRV(src)) != SVt_PVAV) {
// 					croak_cb(cb, "Incosistent type. Expected ARRAYREF");
// 				}
// 				AV *arr = (AV *) src;
// 				uint32_t arr_size = av_len(arr) + 1;
// 				uint32_t i = 0;

// 				for (i = 0; i < arr_size; ++i) {
// 					encode_obj(dest, av_fetch(i), )
// 				}


// 				break;
// 			}
// 			default:
// 				croak_cb(cb,"Unsupported format: %c", format);
// 		}
// }


static inline SV * pkt_ping(uint32_t iid) {
	int sz = HEADER_CONST_LEN;

	create_buffer(rv, h, sz, TP_PING, iid);
	SvCUR_set(rv, sz);
	return rv;
}

static inline SV * pkt_select(TntCtx *ctx, uint32_t iid, HV * spaces, SV *space, SV *keys, HV * opt, SV *cb) {
	U32 limit  = 0xffffffff;
	U32 offset = -1;
	U32 index  = 0;
	U32 flags  = 0;
	U32 iterator = -1;

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
		if ((key = hv_fetchs(opt, "iterator", 0)) && SvOK(*key) && SvPOK(*key)) iterator = get_iterator(*key);
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

	uint32_t body_map_sz = 3 + (index != -1) + (offset != -1) + (iterator != -1);
	uint32_t keys_size = 0;


	int sz = HEADER_CONST_LEN +
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

	if (iterator != -1) {
		sz += mp_sizeof_uint(TP_ITERATOR) +
			  mp_sizeof_uint(iterator);
	}

	sz += mp_sizeof_uint(TP_KEY);


	// counting fields in keys
	SV *t = keys;
	AV *fields;
	if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVHV) {
		fields = hash_to_array_fields( (HV *) SvRV(t), idx->fields, cb );
	} else if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVAV) {
		fields  = (AV *) SvRV(t);
	} else {
		croak_cb(cb, "Input container is invalid. Expecting ARRAYREF or HASHREF");
	}


	// TODO: check_tuple(keys, spc);
	if (unlikely( !keys || !SvROK(keys) || ( (SvTYPE(SvRV(keys)) != SVt_PVAV) && (SvTYPE(SvRV(keys)) != SVt_PVHV) ) )) {
		if (!ctx->f.nofree) safefree(ctx->f.f);
		croak_cb(cb,"keys must be ARRAYREF or HASHREF");
	}

	keys_size = av_len(fields) + 1;
	sz += mp_sizeof_array(keys_size);
	sz += keys_size * EST_FIELD_SIZE;

	create_buffer(rv, h, sz, TP_SELECT, iid);

	h = mp_encode_map(h, body_map_sz);
	h = mp_encode_uint(h, TP_SPACE);
	h = mp_encode_uint(h, spc->id);
	h = mp_encode_uint(h, TP_LIMIT);
	h = mp_encode_uint(h, limit);

	if (index != -1) {
		h = mp_encode_uint(h, TP_INDEX);
		h = mp_encode_uint(h, index);
	}

	if (offset != -1) {
		h = mp_encode_uint(h, TP_OFFSET);
		h = mp_encode_uint(h, offset);
	}

	if (iterator != -1) {
		h = mp_encode_uint(h, TP_ITERATOR);
		h = mp_encode_uint(h, iterator);
	}

	h = mp_encode_uint(h, TP_KEY);
	h = mp_encode_array(h, keys_size);
	encode_keys(h, sz, fields, keys_size, fmt, key);

	char *p = SvPVX(rv);
	write_length(p, h-p-5);
	SvCUR_set(rv, h-p);

	return SvREFCNT_inc(rv);
}

static inline SV * pkt_insert(TntCtx *ctx, uint32_t iid, HV *spaces, SV *space, SV *tuple, HV * opt, SV * cb) {
	uint32_t op_code = TP_INSERT;

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
		if ((key = hv_fetchs(opt, "replace", 0)) && SvOK(*key) && SvIV(*key) != 0) op_code = TP_REPLACE;
		if ((key = hv_fetchs(opt, "hash", 0)) ) ctx->use_hash = SvOK(*key) ? SvIV( *key ) : 0;
	}
	evt_opt_out( opt, ctx, spc );
	check_tuple(tuple, spc);
	evt_opt_in( opt, ctx, spc );


	int sz = HEADER_CONST_LEN +
		1 + // mp_sizeof_map(2) +
		1 + // mp_sizeof_uint(TP_SPACE) +
		mp_sizeof_uint(spc->id) +
		1; // mp_sizeof_uint(TP_TUPLE);

	// counting fields in keys
	SV *t = tuple;
	AV *fields;
	if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVHV) {
		fields = hash_to_array_fields( (HV *) SvRV(t), idx->fields, cb );
	} else if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVAV) {
		fields  = (AV *) SvRV(t);
	} else {
		croak_cb(cb, "Input container is invalid. Expecting ARRAYREF or HASHREF");
	}

	uint32_t cardinality = av_len(fields) + 1;

	sz += mp_sizeof_array(cardinality);
	sz += cardinality * EST_FIELD_SIZE;

	create_buffer(rv, h, sz, op_code, iid);
	h = mp_encode_map(h, 2);
	h = mp_encode_uint(h, TP_SPACE);
	h = mp_encode_uint(h, spc->id);
	h = mp_encode_uint(h, TP_TUPLE);
	h = mp_encode_array(h, cardinality);
	encode_keys(h, sz, fields, cardinality, fmt, key);

	char *p = SvPVX(rv);
	write_length(p, h-p-5);
	SvCUR_set(rv, h-p);

	return SvREFCNT_inc(rv);
}


static inline char * pkt_update_write_tuple(TntCtx *ctx, TntSpace *spc, TntIndex *idx, SV *tuple, uint32_t sz, SV *rv, char *h, SV *cb) {
	SV **key;

	if (unlikely( !tuple || !SvROK(tuple) || (SvTYPE(SvRV(tuple)) != SVt_PVAV))) {
		if (!ctx->f.nofree) safefree(ctx->f.f);
		croak_cb(cb,"tuple must be ARRAYREF");
	}

	AV *t = (AV *) SvRV(tuple);
	uint32_t tuple_size = av_len(t) + 1;

	sz += mp_sizeof_uint(TP_TUPLE)
		+ mp_sizeof_array(tuple_size);

	sv_size_check(rv, h, sz);

	h = mp_encode_uint(h, TP_TUPLE);
	h = mp_encode_array(h, tuple_size);

	SV **operation_sv;
	AV *operation;


	uint32_t i = 0;
	for (i = 0; i < tuple_size; ++i) {
		operation_sv = av_fetch(t, i, 0);

		if (unlikely(!operation_sv || !(*operation_sv) || !SvROK(*operation_sv) || SvTYPE(SvRV(*operation_sv)) != SVt_PVAV)) {
			croak_cb(cb, "tuple\'s element must be a ARRAYREF");
		}

		operation = (AV *) SvRV(*operation_sv);
		if (av_len(operation) < 2) croak_cb(cb, "Too short operation argument list");

		char *op;
		uint32_t field_no;

		key = av_fetch(operation, 0, 0);
		char field_format = 'l';
		if (SvIOK(*key) && SvIVX(*key) >= 0) {
			field_no = SvUV(*key);
			if (spc && field_no < spc->f.size) {
				field_format = spc->f.f[field_no];
			}
		}
		else {
			HE *fhe = hv_fetch_ent(spc->field, *key, 1, 0);
			if (fhe && SvOK( HeVAL(fhe) )) {
				TntField *fld = (TntField *) SvPVX( HeVAL(fhe) );
				field_format = fld->format;
				field_no = fld->id;
			}
			else {
				croak_cb(cb,"Unknown field name: '%s' in space %d",SvPV_nolen( *key ), spc->id);
			}
		}

		op = SvPV_nolen(*av_fetch(operation, 1, 0));

		switch (get_update_op_type(op, 1)) {

			case OP_UPD_ARITHMETIC:
			case OP_UPD_DELETE: {
				int32_t argument = 0;
				if ((key = av_fetch(operation, 2, 0)) && *key && SvIOK(*key)) {
					argument = SvIV(*key);
				} else {
					croak_cb(cb, "Integer argument is required for arithmetic and delete operations");
				}

				sz += mp_sizeof_array(3) +
					  mp_sizeof_str(1) +
					  mp_sizeof_uint(field_no)
					  ;
			  	uint32_t arg_size = 0;
			  	field_size_sv_fmt(arg_size, field_format);
			  	sz += arg_size;

				sv_size_check(rv, h, sz);

				h = mp_encode_array(h, 3);
				h = mp_encode_str(h, op, 1);
				h = mp_encode_uint(h, field_no);
				field_sv_fmt(h, *key, field_format);

				break;
			}

			case OP_UPD_INSERT_ASSIGN: {
				SV *argument;
				if ((key = av_fetch(operation, 2, 0)) && *key && SvOK(*key)) {
					argument = *key;
				} else {
					croak_cb(cb, "Integer argument is required for arithmetic and delete operations");
				}

				sz += mp_sizeof_array(3) +
					  mp_sizeof_str(1) +
					  mp_sizeof_uint(field_no)
					  ;
			  	uint32_t arg_size = 0;
			  	field_size_sv_fmt(arg_size, field_format);
			  	sz += arg_size;

			  	sv_size_check(rv, h, sz);

				h = mp_encode_array(h, 3);
				h = mp_encode_str(h, op, 1);
				h = mp_encode_uint(h, field_no);
				// h = mp_encode_uint(h, argument); // TODO

				break;
			}

			case OP_UPD_SPLICE: {

				uint32_t position;
				uint32_t offset;
				SV *argument;

				if ((key = av_fetch(operation, 2, 0)) && *key && SvIOK(*key)) {
					position = SvUV(*key);
				} else {
					croak_cb(cb, "Position is required for splice operation");
				}

				if ((key = av_fetch(operation, 3, 0)) && *key && SvIOK(*key)) {
					offset = SvUV(*key);
				} else {
					croak_cb(cb, "Offset is required for splice operation");
				}

				if ((key = av_fetch(operation, 4, 0)) && *key && SvOK(*key)) {
					argument = *key;
				} else {
					croak_cb(cb, "Argument is required for splice operation");
				}

				sz += mp_sizeof_array(5) +
					  mp_sizeof_str(1) +
					  mp_sizeof_uint(field_no) +
					  mp_sizeof_uint(position) +
					  mp_sizeof_uint(offset) +
					  mp_sizeof_str(SvCUR(argument));

				sv_size_check(rv, h, sz);

				h = mp_encode_array(h, 5);
				h = mp_encode_str(h, op, 1);
				h = mp_encode_uint(h, field_no);
				h = mp_encode_uint(h, position);
				h = mp_encode_uint(h, offset);
				h = mp_encode_str(h, SvPV_nolen(argument), SvCUR(argument));

				break;
			}

			case OP_UPD_UNKNOWN: {
				croak_cb(cb, "Update operation is unknown");
			}
		}
	}

	return h;
}


static inline SV * pkt_update(TntCtx *ctx, uint32_t iid, HV * spaces, SV *space, SV *keys, SV *tuple, HV * opt, SV *cb) {
	U32 index  = 0;

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

		if ((key = hv_fetchs(opt, "hash", 0)) ) ctx->use_hash = SvOK(*key) ? SvIV( *key ) : 0;
	}
	else {
		ctx->f.size = 0;
		// croak_cb(cb, "No extra params provided");
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

	uint32_t body_map_sz = 3 + (index != -1);
	uint32_t keys_size = 0;


	int sz = HEADER_CONST_LEN +
		mp_sizeof_map(body_map_sz) +
		mp_sizeof_uint(TP_SPACE) +
		mp_sizeof_uint(spc->id);

	if (index != -1) {
		sz += mp_sizeof_uint(TP_INDEX) +
			  mp_sizeof_uint(index);
	}

	sz += mp_sizeof_uint(TP_KEY);


	// counting fields in keys
	SV *t = keys;
	AV *fields;
	if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVHV) {
		fields = hash_to_array_fields( (HV *) SvRV(t), idx->fields, cb );
	} else if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVAV) {
		fields  = (AV *) SvRV(t);
	} else {
		croak_cb(cb, "Input container is invalid. Expecting ARRAYREF or HASHREF");
	}


	// TODO: check_tuple(keys, spc);
	if (unlikely( !keys || !SvROK(keys) || ( (SvTYPE(SvRV(keys)) != SVt_PVAV) && (SvTYPE(SvRV(keys)) != SVt_PVHV) ) )) {
		if (!ctx->f.nofree) safefree(ctx->f.f);
		croak_cb(cb,"keys must be ARRAYREF or HASHREF");
	}

	keys_size = av_len(fields) + 1;
	sz += mp_sizeof_array(keys_size);
	sz += keys_size * EST_FIELD_SIZE;

	create_buffer(rv, h, sz, TP_UPDATE, iid);
	h = mp_encode_map(h, body_map_sz);
	h = mp_encode_uint(h, TP_SPACE);
	h = mp_encode_uint(h, spc->id);

	if (index != -1) {
		h = mp_encode_uint(h, TP_INDEX);
		h = mp_encode_uint(h, index);
	}

	h = mp_encode_uint(h, TP_KEY);
	h = mp_encode_array(h, keys_size);
	encode_keys(h, sz, fields, keys_size, fmt, key);

	h = pkt_update_write_tuple(ctx, spc, idx, tuple, sz, rv, h, cb);

	char *p = SvPVX(rv);
	write_length(p, h-p-5);
	SvCUR_set(rv, h-p);

	return SvREFCNT_inc(rv);
}


static inline SV * pkt_delete(TntCtx *ctx, uint32_t iid, HV *spaces, SV *space, SV *keys, HV * opt, SV * cb) {
	uint32_t index = 0;
	unpack_format *fmt;
	dUnpackFormat( format );

	int k;
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
			warn("No index %d config. Using without formats", index);
		}
	}

	evt_opt_out( opt, ctx, spc );
	evt_opt_in( opt, ctx, idx );


	uint32_t body_map_sz = 2 + (index != -1);
	uint32_t keys_size = 0;


	int sz = HEADER_CONST_LEN +
		mp_sizeof_map(body_map_sz) +
		mp_sizeof_uint(TP_SPACE) +
		mp_sizeof_uint(spc->id);

	if (index != -1) {
		sz += mp_sizeof_uint(TP_INDEX) +
			  mp_sizeof_uint(index);
	}

	sz += mp_sizeof_uint(TP_KEY);

	// counting fields in keys
	SV *t = keys;
	AV *fields;
	if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVHV) {
		fields = hash_to_array_fields( (HV *) SvRV(t), idx->fields, cb );
	} else if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVAV) {
		fields  = (AV *) SvRV(t);
	} else {
		croak_cb(cb, "Input container is invalid. Expecting ARRAYREF or HASHREF");
	}

	// TODO: check_tuple(keys, spc);
	if (unlikely( !keys || !SvROK(keys) || ( (SvTYPE(SvRV(keys)) != SVt_PVAV) && (SvTYPE(SvRV(keys)) != SVt_PVHV) ) )) {
		if (!ctx->f.nofree) safefree(ctx->f.f);
		croak_cb(cb,"keys must be ARRAYREF or HASHREF");
	}

	keys_size = av_len(fields) + 1;
	sz += mp_sizeof_array(keys_size);
	sz += keys_size * EST_FIELD_SIZE;

	create_buffer(rv, h, sz, TP_DELETE, iid);
	h = mp_encode_map(h, body_map_sz);
	h = mp_encode_uint(h, TP_SPACE);
	h = mp_encode_uint(h, spc->id);

	if (index != -1) {
		h = mp_encode_uint(h, TP_INDEX);
		h = mp_encode_uint(h, index);
	}

	h = mp_encode_uint(h, TP_KEY);
	h = mp_encode_array(h, keys_size);
	encode_keys(h, sz, fields, keys_size, fmt, key);

	char *p = SvPVX(rv);
	write_length(p, h-p-5);
	SvCUR_set(rv, h-p);

	return SvREFCNT_inc(rv);
}

static inline SV * pkt_eval(TntCtx *ctx, uint32_t iid, HV * spaces, SV *expression, SV *tuple, HV * opt, SV *cb) {
	U32 index  = 0;

	unpack_format *fmt;
	dUnpackFormat( format );

	int k,i;
	SV **key;

	TntSpace *spc = 0;
	TntIndex *idx = 0;

	// if(( spc = evt_find_space( space, spaces ) )) {
	// 	ctx->space = spc;
	// }
	// else {
	// 	ctx->use_hash = 0;
	// }

	if (opt) {
		if ((key = hv_fetch(opt, "index", 5, 0)) && SvOK(*key)) {
			if(( idx = evt_find_index( spc, key ) ))
				index = idx->id;
		}
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

	uint32_t body_map_sz = 2;
	uint32_t keys_size = 0;

	uint32_t expression_size = SvCUR(expression);

	int sz = HEADER_CONST_LEN +
		mp_sizeof_map(body_map_sz) +
		mp_sizeof_uint(TP_EXPRESSION) +
		mp_sizeof_str(expression_size) +
		mp_sizeof_uint(TP_TUPLE)
		;

	// counting fields in keys
	SV *t = tuple;
	AV *fields;
	if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVHV) {
		fields = hash_to_array_fields( (HV *) SvRV(t), idx->fields, cb );
	} else if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVAV) {
		fields  = (AV *) SvRV(t);
	} else {
		croak_cb(cb, "Input container is invalid. Expecting ARRAYREF or HASHREF");
	}


	// TODO: check_tuple(keys, spc);
	if (unlikely( !tuple || !SvROK(tuple) || ( (SvTYPE(SvRV(tuple)) != SVt_PVAV) && (SvTYPE(SvRV(tuple)) != SVt_PVHV) ) )) {
		if (!ctx->f.nofree) safefree(ctx->f.f);
		croak_cb(cb,"tuple must be ARRAYREF or HASHREF");
	}

	keys_size = av_len(fields) + 1;
	sz += mp_sizeof_array(keys_size);
	sz += keys_size * EST_FIELD_SIZE;

	create_buffer(rv, h, sz, TP_EVAL, iid);
	h = mp_encode_map(h, body_map_sz);
	h = mp_encode_uint(h, TP_EXPRESSION);
	h = mp_encode_str(h, (const char *) SvPV_nolen(expression), expression_size);

	h = mp_encode_uint(h, TP_TUPLE);
	h = mp_encode_array(h, keys_size);
	encode_keys(h, sz, fields, keys_size, fmt, key);

	char *p = SvPVX(rv);
	write_length(p, h-p-5);
	SvCUR_set(rv, h-p);

	return SvREFCNT_inc(rv);
}

static inline SV * pkt_call(TntCtx *ctx, uint32_t iid, HV * spaces, SV *function_name, SV *tuple, HV * opt, SV *cb) {
	U32 index  = 0;

	unpack_format *fmt;
	dUnpackFormat( format );

	int k,i;
	SV **key;

	TntSpace *spc = 0;
	TntIndex *idx = 0;

	// if(( spc = evt_find_space( space, spaces ) )) {
	// 	ctx->space = spc;
	// }
	// else {
	// 	ctx->use_hash = 0;
	// }

	if (opt) {
		if ((key = hv_fetch(opt, "index", 5, 0)) && SvOK(*key)) {
			if(( idx = evt_find_index( spc, key ) ))
				index = idx->id;
		}
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

	uint32_t body_map_sz = 2;
	uint32_t keys_size = 0;

	uint32_t function_name_size = SvCUR(function_name);

	int sz = HEADER_CONST_LEN +
		mp_sizeof_map(body_map_sz) +
		mp_sizeof_uint(TP_FUNCTION) +
		mp_sizeof_str(function_name_size) +
		mp_sizeof_uint(TP_TUPLE)
		;

	// counting fields in keys
	SV *t = tuple;
	AV *fields;
	if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVHV) {
		fields = hash_to_array_fields( (HV *) SvRV(t), idx->fields, cb );
	} else if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVAV) {
		fields  = (AV *) SvRV(t);
	} else {
		croak_cb(cb, "Input container is invalid. Expecting ARRAYREF or HASHREF");
	}


	// TODO: check_tuple(keys, spc);
	if (unlikely( !tuple || !SvROK(tuple) || ( (SvTYPE(SvRV(tuple)) != SVt_PVAV) && (SvTYPE(SvRV(tuple)) != SVt_PVHV) ) )) {
		if (!ctx->f.nofree) safefree(ctx->f.f);
		croak_cb(cb,"tuple must be ARRAYREF or HASHREF");
	}

	keys_size = av_len(fields) + 1;
	sz += mp_sizeof_array(keys_size);
	sz += keys_size * EST_FIELD_SIZE;

	create_buffer(rv, h, sz, TP_CALL, iid);
	h = mp_encode_map(h, body_map_sz);
	h = mp_encode_uint(h, TP_FUNCTION);
	h = mp_encode_str(h, (const char *) SvPVX(function_name), function_name_size);

	h = mp_encode_uint(h, TP_TUPLE);
	h = mp_encode_array(h, keys_size);
	encode_keys(h, sz, fields, keys_size, fmt, key);

	char *p = SvPVX(rv);
	write_length(p, h-p-5);
	SvCUR_set(rv, h-p);

	return SvREFCNT_inc(rv);
}


static inline uint32_t decode_pkt_len(char **h) {
	char *p = *h;
	uint32_t l = *((uint32_t *)(p+1));
	*h += 5;
	return be32toh(l);
}

static int parse_reply_hdr(HV *ret, const char const *data, STRLEN size, uint32_t *id) {
	const char *ptr, *beg, *end;

	const char *p = data;
	const char *test = p;

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
		// cwarn("key = %d", key);
		switch (key) {
			case TP_CODE:
				if (mp_typeof(*p) != MP_UINT)
					return -1;

				code = mp_decode_uint(&p);
				break;

			case TP_SYNC:
				if (mp_typeof(*p) != MP_UINT)
					return -1;


				*id = mp_decode_uint(&p);
				break;
		}
	}
	SV *id_sv = newSVuv(*id);

	cwarn("code: %d; sync: %d", code, (uint32_t) SvUV(id_sv));

	(void) hv_stores(ret, "code", newSVuv(code));
	(void) hv_stores(ret, "sync", id_sv);

	return p - data;
}


static SV* data_parser(const char **p) {
	uint32_t i = 0;
	const char *str = NULL;
	uint32_t str_len = 0;

	switch (mp_typeof(**p)) {
	case MP_UINT: {
		uint32_t value = mp_decode_uint(p);
		return (SV *) newSVuv(value);
	}
	case MP_INT: {
		int32_t value = mp_decode_int(p);
		return (SV *) newSViv(value);
	}
	case MP_STR: {
		str = mp_decode_str(p, &str_len);
		return (SV *) newSVpvn(str, str_len);
	}
	case MP_BOOL: {
		bool value = mp_decode_bool(p);
		if (value) {
			return newSVsv(types_true);
		} else {
			return newSVsv(types_false);
		}
	}
	case MP_FLOAT: {
		float value = mp_decode_float(p);
		return (SV *) newSVnv((double) value);
	}
	case MP_DOUBLE: {
		double value = mp_decode_double(p);
		return (SV *) newSVnv(value);
	}
	case MP_ARRAY: {
		uint32_t arr_size = mp_decode_array(p);

		AV *arr = newAV();
		av_extend(arr, arr_size);
		for (i = 0; i < arr_size; ++i) {
			av_push(arr, data_parser(p));
		}
		return newRV_noinc((SV *) arr);
	}

	case MP_MAP: {
		uint32_t map_size = mp_decode_map(p);
		// cwarn("map_size = %d", map_size);

		const char *map_key_str = NULL;
		uint32_t map_key_len = 0;

		HV *hash = newHV();
		for (i = 0; i < map_size; ++i) {
			bool _set = true;
			SV *key;
			switch(mp_typeof(**p)) {
			case MP_STR: {
				map_key_str = mp_decode_str(p, &map_key_len);
				break;
			}
			case MP_UINT: {
				uint32_t value = mp_decode_uint(p);
				SV *s = newSVuv(value);
				STRLEN l;

				map_key_str = SvPV(s, l);
				map_key_len = (uint32_t) l;
				break;
			}
			case MP_INT: {
				int32_t value = mp_decode_int(p);
				SV *s = newSViv(value);
				STRLEN l;

				map_key_str = SvPV(s, l);
				map_key_len = (uint32_t) l;
				break;
			}
			default:
				_set = false;
				break;
			}
			if (_set) {
				SV *value = data_parser(p);
				(void) hv_store(hash, map_key_str, map_key_len, value, 0);
			} else {
				mp_next(p); // skip the current key
				mp_next(p); // skip the value of current key
			}
		}
		return newRV_noinc((SV *) hash);
	}
	case MP_NIL:
		mp_next(p);
		return &PL_sv_undef;
	default:
		warn("Got unexpected type as a tuple element value");
		mp_next(p);
		return &PL_sv_undef;
	}
}

static inline int parse_reply_body_data(HV *ret, const char const *data_begin, const char const *data_end, const unpack_format const * format, AV *fields) {
	STRLEN data_size = data_end - data_begin;
	if (data_size == 0)
		return 0;

	const char *p = data_begin;

	uint32_t cont_size = 0;
	switch (mp_typeof(*p)) {
	case MP_MAP: {
		cont_size = mp_decode_map(&p);
		cwarn("map.size=%d", cont_size);
		// TODO: this is not valid probably
		break;
	}

	case MP_ARRAY: {
		cont_size = mp_decode_array(&p);
		cwarn("tuples count = %d", cont_size);

		AV *tuples = newAV();
		av_extend(tuples, cont_size);
		(void) hv_stores(ret, "count", newSViv(cont_size));
		(void) hv_stores(ret, "tuples", newRV_noinc((SV *) tuples ));

		uint32_t tuple_size = 0;
		uint32_t i = 0, k = 0;
		if (fields) { // using space definition
			uint32_t known_tuple_size = av_len(fields) + 1;
			SV **name;
			for (i = 0; i < cont_size; ++i) {
				HV *tuple = newHV();
				AV *unknown_fields = NULL;
				av_push(tuples, newRV_noinc((SV *)tuple));

				tuple_size = mp_decode_array(&p);

				for (k = 0; k < tuple_size; ++k) {
					SV *field_value = data_parser(&p);

					if (k < known_tuple_size && (name = av_fetch(fields, k, 0)) && *name) {
						(void) hv_store(tuple, SvPV_nolen(*name), sv_len(*name), field_value, 0);
					} else {
						cwarn("Field name for field %d is not defined", k);
						if (unknown_fields == NULL) {
							unknown_fields = newAV();
						}
						av_push(unknown_fields, field_value);
					}
				}

				if (unknown_fields != NULL) {
					(void) hv_stores(tuple, "", newRV_noinc((SV *) unknown_fields));
				}
			}

		} else {  // without space definition
			for (i = 0; i < cont_size; ++i) {
				AV* tuple = newAV();
				av_push(tuples, newRV_noinc((SV *)tuple));
				av_extend(tuple, tuple_size);

				tuple_size = mp_decode_array(&p);

				for (k = 0; k < tuple_size; ++k) {
					(void) av_push(tuple, data_parser(&p));
				}
			}
		}

		break;
	}
	default:
		cwarn("response data type = %d", mp_typeof(*p));
		break;
	}

	return 0;
}

static inline int parse_spaces_body_data(HV *ret, const char const *data_begin, const char const *data_end) {
	uint32_t VALID_TUPLE_SIZE = 7;

	STRLEN data_size = data_end - data_begin;
	if (data_size == 0)
		return 0;

	const char *p = data_begin;

	uint32_t cont_size = 0;
	switch (mp_typeof(*p)) {
	case MP_MAP: {
		cont_size = mp_decode_map(&p);
		cwarn("map.size=%d", cont_size);
		// TODO: this is not valid probably
		break;
	}

	case MP_ARRAY: {
		cont_size = mp_decode_array(&p);
		cwarn("tuples count = %d", cont_size);

		HV *data = newHV();

		(void) hv_stores(ret, "count", newSViv(cont_size));
		(void) hv_stores(ret, "data", newRV_noinc((SV *) data));

		// AV *tuples = newAV();
		// av_extend(tuples, cont_size);


		uint32_t tuple_size = 0;
		uint32_t i = 0, k;

		for (i = 0; i < cont_size; ++i) {
			k = 1;
			HV *sp = newHV();

			tuple_size = mp_decode_array(&p);
			// cwarn("tuple_size = %d", tuple_size);

			if (tuple_size < 1) {
				warn("Invalid tuple size. Should be %d. Got %d. Exiting", VALID_TUPLE_SIZE, tuple_size);
				return 0;
			}

			if (tuple_size != VALID_TUPLE_SIZE) {
				warn("Invalid tuple size. Should be %d. Got %d", VALID_TUPLE_SIZE, tuple_size);
			}

			SV *sv_id = data_parser(&p);
			int id = (U32) SvUV(sv_id);
			// cwarn("space_id=%d", id);

			SV *spcf = newSV( sizeof(TntSpace) );

			SvUPGRADE(spcf, SVt_PV);
			SvCUR_set(spcf, sizeof(TntSpace));
			SvPOKp_on(spcf);
			TntSpace *spc = (TntSpace *) SvPVX(spcf);
			memset(spc, 0, sizeof(TntSpace));

			(void) hv_store( data, (char *) &id, sizeof(U32), spcf, 0);

			spc->id = id;
			spc->indexes = newHV();
			spc->field   = newHV();
			spc->fields  = newAV();

			spc->f.nofree = 1;
			spc->f.def = '*';

			++k; if (k <= tuple_size) spc->owner = data_parser(&p);
			++k; if (k <= tuple_size) spc->name = data_parser(&p);
			++k; if (k <= tuple_size) spc->engine = data_parser(&p);
			++k; if (k <= tuple_size) spc->fields_count = data_parser(&p);
			++k; if (k <= tuple_size) spc->flags = data_parser(&p);
			++k; if (k <= tuple_size) {                  			// format

				uint32_t str_len = 0;
				uint32_t format_arr_size = mp_decode_array(&p);

				spc->f.size = format_arr_size + 1;
				spc->f.f = safemalloc(spc->f.size + 1);
				spc->f.f[spc->f.size] = 0;

				uint32_t ix;
				for (ix = 0; ix < format_arr_size; ++ix) {
					uint32_t field_format_map_size = mp_decode_map(&p);
					if (field_format_map_size != 2) {
						// TODO: error!
						cwarn("Bad things happened! field_format_map_size != 2");
					}

					SV *field_name;
					uint32_t field_name_len = 0;
					while (field_format_map_size-- > 0) {

						const char *str = mp_decode_str(&p, &str_len);

						if (str_len == 4 && strncasecmp(str, "name", 4) == 0) {
							str = mp_decode_str(&p, &str_len); // getting the name itself
							field_name = newSVpvn(str, str_len);
							field_name_len = str_len;
						}
						else
						if (str_len == 4 && strncasecmp(str, "type", 4) == 0) {
							str = mp_decode_str(&p, &str_len); // getting the type itself

							if (str_len == 3 && strncasecmp(str, "STR", 3) == 0) {
								spc->f.f[ix] = 'p';
							}
							else
							if (str_len == 3 && strncasecmp(str, "NUM", 3) == 0) {
								spc->f.f[ix] = 'L';
							}
							else
							if (str_len == 1 && strncasecmp(str, "*", 1) == 0) {
								spc->f.f[ix] = '*';
							}
						}
					}

					dSVX(fldsv, fld, TntField);
					fld->id = ix;
					fld->format = spc->f.f[ix];
					fld->name = field_name;
					(void) hv_store(spc->field, SvPV_nolen(field_name), field_name_len, fldsv, 0);
					av_push(spc->fields, SvREFCNT_inc(field_name));
				}
			}

			(void) hv_store(data, SvPV_nolen(spc->name), SvCUR(spc->name), SvREFCNT_inc(spcf),0);
		}

		break;
	}
	default:
		cwarn("response data type = %d", mp_typeof(*p));
		break;
	}

	return 0;
}

static inline int parse_index_body_data(HV *spaces, const char const *data_begin, const char const *data_end) {
	uint32_t VALID_TUPLE_SIZE = 7;

	STRLEN data_size = data_end - data_begin;
	if (data_size == 0)
		return 0;

	const char *p = data_begin;

	uint32_t cont_size = 0;
	switch (mp_typeof(*p)) {
	case MP_MAP: {
		cont_size = mp_decode_map(&p);
		cwarn("map.size=%d", cont_size);
		// TODO: this is not valid probably
		break;
	}

	case MP_ARRAY: {
		cont_size = mp_decode_array(&p);
		cwarn("tuples count = %d", cont_size);

		// HV *data = newHV();

		// (void) hv_stores(ret, "count", newSViv(cont_size));
		// (void) hv_stores(ret, "data", newRV_noinc((SV *) data));

		// AV *tuples = newAV();
		// av_extend(tuples, cont_size);


		uint32_t tuple_size = 0;
		uint32_t i = 0, k;

		SV **key;
		for (i = 0; i < cont_size; ++i) {

			tuple_size = mp_decode_array(&p);
			// cwarn("tuple_size = %d", tuple_size);

			uint32_t space_id = mp_decode_uint(&p);
			uint32_t index_id = mp_decode_uint(&p);

			if ((key = hv_fetch(spaces, (char *) &space_id, sizeof(uint32_t), 0)) && *key) {
				TntSpace* spc = (TntSpace*) SvPVX(*key);

				SV *idxcf = newSV(sizeof(TntIndex));
				SvUPGRADE(idxcf, SVt_PV);
				SvCUR_set(idxcf, sizeof(TntIndex));
				SvPOKp_on(idxcf);
				TntIndex *idx = (TntIndex *) SvPVX(idxcf);
				memset(idx, 0, sizeof(TntIndex));

				idx->id = index_id;
				idx->name = data_parser(&p);
				idx->type = data_parser(&p);
				idx->unique = mp_decode_uint(&p);

				if (mp_typeof(*p) != MP_UINT) {
					croak("parts count has to be uint");
				}
				uint32_t parts_count = mp_decode_uint(&p);

				idx->f.nofree = 1;
				idx->f.size = parts_count;
				idx->f.f = safemalloc(idx->f.size+1);
				idx->f.f[idx->f.size] = 0;
				idx->f.def = '*';
				idx->fields = newAV();
				av_extend(idx->fields, parts_count);

				uint32_t part_i;
				uint32_t ix = 0;
				const char *str;
				uint32_t str_len;

				for (part_i = 0; part_i < parts_count; ++part_i) {
					ix = mp_decode_uint(&p);
					str = mp_decode_str(&p, &str_len);

					if (str_len == 3 && strncasecmp(str, "STR", 3) == 0) {
						idx->f.f[part_i] = 'p';
					}
					else
					if (str_len == 3 && strncasecmp(str, "NUM", 3) == 0) {
						idx->f.f[part_i] = 'L';
					}
					else
					if (str_len == 1 && strncasecmp(str, "*", 1) == 0) {
						idx->f.f[part_i] = '*';
					}

					SV **f = av_fetch(spc->fields, ix, 0);
					if (!f) {
						// if this field is not in space format information
						croak("this field is not in space format information");
					}
					HE *fhe = hv_fetch_ent(spc->field, *f, 1, 0);
					if (SvOK( HeVAL(fhe) )) {
						av_push(idx->fields, SvREFCNT_inc(((TntField *)SvPVX( HeVAL(fhe) ))->name));
						// idx->f.f[ix] = ((TntField *)SvPVX( HeVAL(fhe) ))->format;
					}
				}


				(void) hv_store(spc->indexes, (char *) &index_id, sizeof(uint32_t), idxcf, 0);
				(void) hv_store(spc->indexes, SvPV_nolen(idx->name), SvCUR(idx->name), SvREFCNT_inc(idxcf), 0);

			} else {
				cwarn("Space definition not found for space %d", space_id);
			}
		}

		break;
	}
	default:
		cwarn("response data type = %d", mp_typeof(*p));
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


static int parse_spaces_body(HV *ret, const char const *data, STRLEN size) {
	// TODO: COPY AND THE F*CKING PASTE

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
			parse_spaces_body_data(ret, data_begin, p);


			break;
		}
		}
		// r->bitmap |= (1ULL << key);
	}
	return p - data;
}

static int parse_index_body(HV *spaces, const char const *data, STRLEN size) {
	// TODO: COPY AND THE F*CKING PASTE

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

			// TODO: notify about an error

			// (void) hv_stores(ret, "status", newSVpvs("error"));
			// (void) hv_stores(ret, "errstr", newSVpvn(err_str, elen));
			break;
		}

		case TP_DATA: {
			if (mp_typeof(*p) != MP_ARRAY)
				return -1;

			// (void) hv_stores(ret, "status", newSVpvs("ok"));
			const char *data_begin = p;
			mp_next(&p);
			parse_index_body_data(spaces, data_begin, p);


			break;
		}
		}
		// r->bitmap |= (1ULL << key);
	}
	return p - data;
}
