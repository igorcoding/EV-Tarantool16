#ifndef _XSTNT16_H_
#define _XSTNT16_H_

#define MP_SOURCE 1

#include <string.h>
#include <stdbool.h>
#include "xsmy.h"
#include "msgpuck.h"
#include "types.h"
#include "encdec.h"
#include "sha1.h"
#include "base64.h"
#include "log.h"

static const uint32_t SCRAMBLE_SIZE = 20;
static const uint32_t HEADER_CONST_LEN = 5 + // pkt_len
                                         1 + // mp_sizeof_map(2) +
                                         1 + // mp_sizeof_uint(TP_CODE) +
                                         1 + // mp_sizeof_uint(TP COMMAND) +
                                         1 + // mp_sizeof_uint(TP_SYNC) +
                                         5;  // sync len


#define check_tuple(tuple, allow_hash, cb) STMT_START { \
	if (SvROK(tuple)) { \
		if ( SvTYPE(SvRV(tuple)) == SVt_PVHV ) { \
			if (unlikely(!(allow_hash))) { \
				croak_cb(cb,"Cannot use hash without space or index"); \
			} \
		} else if ( unlikely(SvTYPE(SvRV(tuple)) != SVt_PVAV) ) { \
			croak_cb(cb,"Tuple must be %s, but have %s", (allow_hash) ? "ARRAYREF or HASHREF" : "ARRAYREF", SvPV_nolen(tuple) ); \
		} \
	} else { \
		croak_cb(cb,"Tuple must be %s, but have %s", (allow_hash) ? "ARRAYREF or HASHREF" : "ARRAYREF", SvPV_nolen(tuple) ); \
	} \
} STMT_END



#define dUnpackFormat(fvar) unpack_format fvar; fvar.f = ""; fvar.nofree = 1; fvar.size = 0; fvar.def = FMT_UNKNOWN

static TntIndex *evt_find_index(TntSpace *spc, SV **key, uint8_t log_level) {
	if (SvIOK(*key)) {
		int iid = SvUV(*key);
		if ((key = hv_fetch(spc->indexes, (char *)&iid, sizeof(U32), 0)) && *key) {
			return (TntIndex *) SvPVX(*key);
		} else {
			log_warn(log_level, "Unknown index %d in space \'%s\' [%d]", iid, SvPV_nolen(spc->name), spc->id);
			return NULL;
		}
	} else {
		if ((key = hv_fetch(spc->indexes, SvPV_nolen(*key), SvCUR(*key), 0)) && *key) {
			return (TntIndex *) SvPVX(*key);
		} else {
			log_warn(log_level, "Unknown index \'%s\' in space \'%s\' [%d]", SvPV_nolen(*key), SvPV_nolen(spc->name), spc->id);
			return NULL;
		}
	}

}

static TntSpace *evt_find_space(SV *space, HV *spaces, uint8_t log_level, SV *cb) {
	U32 ns;
	SV **key;
	if (SvIOK(space)) {
		ns = SvUV(space);
		if ((key = hv_fetch(spaces, (char *) &ns, sizeof(U32),0)) && *key) {
			return (TntSpace *) SvPVX(*key);
		} else {
			if (spaces != NULL) {
				log_info(log_level, "No space config found for space %u. Creating dummy space.", ns);
				dSVX(spcf, spc, TntSpace);

				spc->id = ns;
				spc->name = newSVpvf("%u", ns);
				spc->f.def = FMT_UNKNOWN;

				(void)hv_store(spaces, (char *)&ns, sizeof(U32), spcf, 0);
				(void)hv_store(spaces, SvPV_nolen(spc->name), SvLEN(spc->name), SvREFCNT_inc(spcf), 0);
				return spc;
			} else {
				log_error(log_level, "No spaces are defined");
				return NULL;
			}
		}
	}
	else if (SvPOK(space)) {
		if ((key = hv_fetch(spaces, SvPV_nolen(space), SvCUR(space), 0)) && *key) {
			return (TntSpace *) SvPVX(*key);
		} else {
			croak_cb(cb, "Unknown space %s", SvPV_nolen(space));
		}
	} else {
		croak_cb(cb, "Space must be either a string or a number");
	}
}

static void destroy_spaces(HV *spaces) {
	debug("Destroy started");
	HE *ent;
	(void) hv_iterinit(spaces);
	while ((ent = hv_iternext(spaces))) {
		TntSpace *spc = (TntSpace *) SvPVX( HeVAL(ent) );
		HE *he;
		if (spc->name) {
			// cwarn("destroy space %d:%s",spc->id,SvPV_nolen(spc->name));
			SvREFCNT_dec(spc->name);
			spc->name = NULL;
		}

		if (spc->fields) {
			SvREFCNT_dec(spc->fields);
			spc->fields = NULL;
		}
		if (spc->field) {
			SvREFCNT_dec(spc->field);
			spc->field = NULL;
		}
		if (spc->indexes) {
			//cwarn("des idxs, refs = %d", SvREFCNT( spc->indexes ));
			(void) hv_iterinit( spc->indexes );
			while ((he = hv_iternext( spc->indexes ))) {
				TntIndex *idx = (TntIndex *) SvPVX(HeVAL(he));
				if (idx->name) {
					//cwarn("destroy index %s in space %s",SvPV_nolen(idx->name), SvPV_nolen(spc->name));
					if (idx->f.size > 0) safefree(idx->f.f);
					if (idx->fields) SvREFCNT_dec(idx->fields);
					SvREFCNT_dec(idx->name);
					idx->name = NULL;
					
					if (idx->type) {
						SvREFCNT_dec(idx->type);
						idx->type = NULL;
					}
					
					if (idx->opts) {
						SvREFCNT_dec(idx->opts);
						idx->opts = NULL;
					}
				}
			}
			SvREFCNT_dec(spc->indexes);
			spc->indexes = NULL;
		}

		if (spc->f.size) {
			safefree(spc->f.f);
			spc->f.f = NULL;
			spc->f.size = 0;
		}

		if (spc->owner) {
			SvREFCNT_dec(spc->owner);
			spc->owner = NULL;
		}
		if (spc->engine) {
			SvREFCNT_dec(spc->engine);
			spc->engine = NULL;
		}
		if (spc->fields_count) {
			SvREFCNT_dec(spc->fields_count);
			spc->fields_count = NULL;
		}
		if (spc->flags) {
			SvREFCNT_dec(spc->flags);
			spc->flags = NULL;
		}
	}
	SvREFCNT_dec(spaces);
}

#define CHECK_PACK_FORMAT(src, cb) STMT_START { \
	char *p = src; \
	while(*p) { \
		switch(*p) { \
			case FMT_UNKNOWN: \
			case FMT_UNSIGNED: \
			case FMT_STRING: \
			case FMT_NUMBER: \
			case FMT_INTEGER: \
			case FMT_ARRAY: \
			case FMT_SCALAR: \
				p++; break; \
			default: \
				croak_cb(cb,"Unknown pattern in format: %c", *p); \
		} \
	} \
} STMT_END

#define dExtractFormat2(fvar, src, cb) STMT_START { \
	if (SvOK(src) && SvPOK(src)) { \
		fvar.f = SvPVbyte(src, fvar.size); \
		CHECK_PACK_FORMAT(fvar.f, cb); \
	} else if (!SvOK(src)) { \
	} else { \
		croak_cb(cb,"Usage { .. in => 'fmtstring', .. }"); \
	} \
} STMT_END

#define evt_opt_in(opt, idx, key, format, fmt, cb) STMT_START { \
	if (opt && (key = hv_fetchs(opt,"in",0)) && *key) { \
		dExtractFormat2( format, *key, cb ); \
		fmt = &format; \
	} else if (idx) { \
		fmt = &idx->f; \
	} else { \
		fmt = &format; \
	} \
} STMT_END


static AV *hash_to_array_fields(HV *hf, AV *fields, bool ignore_missing_fields, SV *cb) {
	AV *rv = (AV *) sv_2mortal((SV *) newAV());
	int fcnt = HvTOTALKEYS(hf);
	int k;

	SV **f;
	HE *fl;

	for (k = 0; k <= av_len(fields);k++) {
		f = av_fetch(fields, k, 0);
		if (unlikely(!f)) {
			croak_cb(cb, "Missing field %d entry", k);
		}
		fl = hv_fetch_ent(hf,*f,0,0);
		if (fl && SvOK( HeVAL(fl) )) {
			fcnt--;
			av_push( rv, SvREFCNT_inc_NN(HeVAL(fl)) );
		} else {
			// cwarn("missing field: %.*s", SvCUR(*f), SvPV_nolen(*f));
			if (!ignore_missing_fields) {
				av_push( rv, &PL_sv_undef );
			}
		}
	}
	if (unlikely(fcnt != 0)) {
		HV *used = (HV *) sv_2mortal((SV *) newHV());
		for (k = 0; k <= av_len( fields );k++) {
			f = av_fetch( fields,k,0 );
			fl = hv_fetch_ent(hf,*f,0,0);
			if (fl && SvOK( HeVAL(fl) )) {
				(void) hv_store(used, SvPV_nolen(*f), sv_len(*f), &PL_sv_undef, 0);
			}
		}
		if ((f = hv_fetch(hf, "", 0, 0)) && SvROK(*f)) {
			(void) hv_store(used, "", 0, &PL_sv_undef, 0);
		}
		(void) hv_iterinit( hf );
		STRLEN nlen;
		while ((fl = hv_iternext( hf ))) {
			char *name = HePV(fl, nlen);
			if (!hv_exists(used, name, nlen)) {
				warn("tuple key = %s; val = %s could not be used in hash fields",name, SvPV_nolen(HeVAL(fl)));
			}
		}
	}
	return rv;
}

#define COMP_STR(lhs, rhs, lhs_len, rhs_len, res) STMT_START { \
	if ((lhs_len) == (rhs_len) && strncasecmp((lhs), (rhs), rhs_len) == 0) { \
		return (res); \
	} \
} STMT_END


static inline U32 get_iterator(TntCtx *ctx, SV *iterator_str) {
	const char *str = SvPVX(iterator_str);
	U32 str_len = SvCUR(iterator_str);
	COMP_STR(str, "EQ",               str_len, 2,  TNT_IT_EQ);
	COMP_STR(str, "REQ",              str_len, 3,  TNT_IT_REQ);
	COMP_STR(str, "ALL",              str_len, 3,  TNT_IT_ALL);
	COMP_STR(str, "LT",               str_len, 2,  TNT_IT_LT);
	COMP_STR(str, "LE",               str_len, 2,  TNT_IT_LE);
	COMP_STR(str, "GE",               str_len, 2,  TNT_IT_GE);
	COMP_STR(str, "GT",               str_len, 2,  TNT_IT_GT);
	COMP_STR(str, "BITS_ALL_SET",     str_len, 12, TNT_IT_BITS_ALL_SET);
	COMP_STR(str, "BITS_ANY_SET",     str_len, 12, TNT_IT_BITS_ANY_SET);
	COMP_STR(str, "BITS_ALL_NOT_SET", str_len, 16, TNT_IT_BITS_ALL_NOT_SET);
	COMP_STR(str, "OVERLAPS",         str_len, 8,  TNT_IT_OVERLAPS);
	COMP_STR(str, "NEIGHBOR",         str_len, 8,  TNT_IT_NEIGHBOR);
	log_error(ctx->log_level, "Unknown iterator: %.*s", (int) str_len, str);
	return -1;
}


static inline update_op_type_t get_update_op_type(const char *op_str, uint32_t len) {
	if (unlikely(len != 1)) {
		return OP_UPD_UNKNOWN;
	}

	char op = op_str[0];
	
	switch (op) {
		case '+':
		case '-':
		case '&':
		case '^':
		case '|':
			return OP_UPD_ARITHMETIC;
		case '#':
			return OP_UPD_DELETE;
		case '!':
		case '=':
			return OP_UPD_INSERT_ASSIGN;
		case ':':
			return OP_UPD_SPLICE;
		default:
			return OP_UPD_UNKNOWN;
	}
}

static inline SV *pkt_authenticate(uint32_t iid, SV *username, SV *password, const char *const salt_begin, const char *const salt_end, SV *cb) {
	char scramble[SCRAMBLE_SIZE];

	const size_t salt_size = 64;
	char salt[salt_size];
	base64_decode(salt_begin, (int) (salt_end - salt_begin), salt, salt_size);

	unsigned char hash1[SCRAMBLE_SIZE];
	unsigned char hash2[SCRAMBLE_SIZE];
	SHA1_CTX ctx;

	SHA1Init(&ctx);
	SHA1Update(&ctx, (const unsigned char *) SvPV_nolen(password), SvCUR(password));
	SHA1Final(hash1, &ctx);

	SHA1Init(&ctx);
	SHA1Update(&ctx, hash1, SCRAMBLE_SIZE);
	SHA1Final(hash2, &ctx);

	SHA1Init(&ctx);
	SHA1Update(&ctx, (const unsigned char *) salt, SCRAMBLE_SIZE);
	SHA1Update(&ctx, hash2, SCRAMBLE_SIZE);
	SHA1Final((unsigned char *) scramble, &ctx);

	for (int i = 0; i < SCRAMBLE_SIZE; ++i) {
	    scramble[i] = hash1[i] ^ scramble[i];
	}

	size_t sz = HEADER_CONST_LEN
	            + 1  // mp_sizeof_map(2)
	            + 1  // mp_sizeof_uint(TP_USERNAME)
	            + mp_sizeof_str(SvCUR(username))
	            + 1  // mp_sizeof_uint(TP_TUPLE)
	            + 1  // mp_sizeof_array(2)
	            + 1 + 9  // mp_sizeof_str(9)
	            + 1 + SCRAMBLE_SIZE // mp_sizeof_str(SCRAMBLE_SIZE)
	            ;
	create_buffer(rv, h, sz, TP_AUTH, iid);

	h = mp_encode_map(h, 2);
	h = mp_encode_uint(h, TP_USERNAME);
	h = mp_encode_str(h, SvPV_nolen(username), SvCUR(username));
	h = mp_encode_uint(h, TP_TUPLE);
	h = mp_encode_array(h, 2);
	h = mp_encode_str(h, "chap-sha1", 9);
	h = mp_encode_str(h, scramble, SCRAMBLE_SIZE);

	char *p = SvPVX(rv);
	write_length(p, h-p-5);
	SvCUR_set(rv, h-p);
	return SvREFCNT_inc(rv);
}


static inline SV *pkt_ping(uint32_t iid) {
	size_t sz = HEADER_CONST_LEN;

	create_buffer(rv, h, sz, TP_PING, iid);

	char *p = SvPVX(rv);
	write_length(p, h-p-5);
	SvCUR_set(rv, h-p);
	return SvREFCNT_inc(rv);
}

static inline SV *pkt_select(TntCtx *ctx, uint32_t iid, HV *spaces, SV *space, SV *keys, HV *opt, SV *cb) {
	U32 limit  = 0xffffffff;
	U32 offset = -1;
	U32 index  = 0;
	// U32 flags  = 0;
	U32 iterator = -1;

	unpack_format *fmt;
	dUnpackFormat(format);

	SV **key;

	TntSpace *spc = NULL;
	TntIndex *idx = NULL;

	if (( spc = evt_find_space(space, spaces, ctx->log_level, cb) )) {
		ctx->space = spc;
	} else {
		ctx->use_hash = 0;
		return NULL;
	}

	if (opt) {
		if ((key = hv_fetch(opt, "index", 5, 0)) && SvOK(*key)) {
			if(( idx = evt_find_index( spc, key, ctx->log_level ) ))
				index = idx->id;
		}
		if ((key = hv_fetchs(opt, "limit", 0)) && SvOK(*key)) limit = SvUV(*key);
		if ((key = hv_fetchs(opt, "offset", 0)) && SvOK(*key)) offset = SvUV(*key);
		if ((key = hv_fetchs(opt, "iterator", 0)) && SvOK(*key) && SvPOK(*key)) iterator = get_iterator(ctx, *key);
		if ((key = hv_fetchs(opt, "hash", 0)) ) ctx->use_hash = SvOK(*key) ? SvIV( *key ) : 0;
	} else {
		ctx->f.size = 0;
	}
	if (!idx) {
		if ( spc && spc->indexes && (key = hv_fetch(spc->indexes, (char *) &index, sizeof(U32), 0)) && *key) {
			idx = (TntIndex *) SvPVX(*key);
		}
	}
	evt_opt_in(opt, idx, key, format, fmt, cb);

	uint32_t body_map_sz = 3 + (index != -1) + (offset != -1) + (iterator != -1);
	uint32_t keys_size = 0;

	size_t sz = HEADER_CONST_LEN
	            + 1 // mp_sizeof_map(body_map_sz)
	            + 1 // mp_sizeof_uint(TP_SPACE)
	            + 9 // mp_sizeof_uint(spc->id)
	            + 1 // mp_sizeof_uint(TP_LIMIT)
	            + 9; // mp_sizeof_uint(limit);

	if (index != -1) {
		sz += 1 // mp_sizeof_uint(TP_INDEX)
	        + 9; // mp_sizeof_uint(index);
	}

	if (offset != -1) {
		sz += 1 // mp_sizeof_uint(TP_OFFSET)
		    + 9; // mp_sizeof_uint(offset);
	}

	if (iterator != -1) {
		sz += 1 // mp_sizeof_uint(TP_ITERATOR)
		    + 1; // mp_sizeof_uint(iterator); // 1 is because of tnt_iterator_t
	}

	sz += 1; // mp_sizeof_uint(TP_KEY);


	SV *t = keys;
	AV *fields;
	if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVHV) {
		fields = hash_to_array_fields( (HV *) SvRV(t), idx->fields, true, cb );
	} else if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVAV) {
		fields  = (AV *) SvRV(t);
	} else {
		if (!ctx->f.nofree) safefree(ctx->f.f);
		croak_cb(cb, "Input container is invalid. Expecting ARRAYREF or HASHREF");
	}

	keys_size = av_len(fields) + 1;
	sz += mp_sizeof_array(keys_size);

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

static inline SV *pkt_insert(TntCtx *ctx, uint32_t iid, HV *spaces, SV *space, SV *tuple, HV *opt, SV *cb) {
	uint32_t op_code = TP_INSERT;

	unpack_format *fmt;
	dUnpackFormat(format);

	SV **key;
	TntSpace *spc = NULL;

	if(( spc = evt_find_space(space, spaces, ctx->log_level, cb) )) {
		ctx->space = spc;
	}
	else {
		ctx->use_hash = 0;
		return NULL;
	}

	if (opt) {
		if ((key = hv_fetchs(opt, "replace", 0)) && SvOK(*key) && SvIV(*key) != 0) op_code = TP_REPLACE;
		if ((key = hv_fetchs(opt, "hash", 0)) ) ctx->use_hash = SvOK(*key) ? SvIV( *key ) : 0;
	}
	check_tuple(tuple, spc, cb);
	evt_opt_in(opt, spc, key, format, fmt, cb);


	size_t sz = HEADER_CONST_LEN
	            + 1 // mp_sizeof_map(2)
	            + 1 // mp_sizeof_uint(TP_SPACE)
	            + mp_sizeof_uint(spc->id)
	            + 1; // mp_sizeof_uint(TP_TUPLE);

	SV *t = tuple;
	AV *fields;
	if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVHV) {
		fields = hash_to_array_fields( (HV *) SvRV(t), spc->fields, false, cb );
	} else if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVAV) {
		fields  = (AV *) SvRV(t);
	} else {
		croak_cb(cb, "Input container is invalid. Expecting ARRAYREF or HASHREF");
	}

	uint32_t cardinality = av_len(fields) + 1;

	sz += mp_sizeof_array(cardinality);

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


static inline char *pkt_update_write_operations(TntCtx *ctx,
                                                TntSpace *spc,
                                                TntIndex *idx,
                                                uint8_t operations_key,
                                                SV *operations,
                                                size_t *sz,
                                                SV *rv,
                                                char *h,
                                                SV *cb) {
	SV **key;

	if (unlikely( !operations || !SvROK(operations) || (SvTYPE(SvRV(operations)) != SVt_PVAV))) {
		if (!ctx->f.nofree) safefree(ctx->f.f);
		croak_cb(cb, "update operations must be ARRAYREF");
	}

	AV *t = (AV *) SvRV(operations);
	uint32_t tuple_size = av_len(t) + 1;

	*sz += 1 // mp_sizeof_uint(operations_key)
	     + mp_sizeof_array(tuple_size);

	sv_size_check(rv, h, *sz);

	h = mp_encode_uint(h, operations_key);
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
		if (av_len(operation) < 2) {
			croak_cb(cb, "Too short operation argument list");
		}

		char *op;
		uint32_t field_no;

		key = av_fetch(operation, 0, 0);
		char field_format = FMT_UNKNOWN;
		if (SvIOK(*key) && SvIVX(*key) >= 0) {
			field_no = SvUV(*key);
			if (spc && field_no < spc->f.size) {
				field_format = spc->f.f[field_no];
			}
		} else {
			HE *fhe = hv_fetch_ent(spc->field, *key, 1, 0);
			if (fhe && SvOK( HeVAL(fhe) )) {
				TntField *fld = (TntField *) SvPVX( HeVAL(fhe) );
				field_format = fld->format;
				field_no = fld->id;
			} else {
				croak_cb(cb, "Unknown field name: '%s' in space %u", SvPV_nolen( *key ), spc->id);
			}
		}

		op = SvPV_nolen(*av_fetch(operation, 1, 0));

		switch (get_update_op_type(op, 1)) {
			case OP_UPD_ARITHMETIC:
			case OP_UPD_DELETE: {
				SV *argument = 0;
				if ((key = av_fetch(operation, 2, 0)) && *key && SvOK(*key)) {
					argument = *key;
				} else {
					croak_cb(cb, "Integer argument is required for arithmetic and delete operations");
				}

				*sz += 1 // mp_sizeof_array(3)
				     + 2 // mp_sizeof_str(1)
				     + mp_sizeof_uint(field_no)
				;

				sv_size_check(rv, h, *sz);

				h = mp_encode_array(h, 3);
				h = mp_encode_str(h, op, 1);
				h = mp_encode_uint(h, field_no);
				h = encode_obj(argument, h, rv, sz, field_format);

				break;
			}

			case OP_UPD_INSERT_ASSIGN: {
				SV *argument;
				if ((key = av_fetch(operation, 2, 0)) && *key && SvOK(*key)) {
					argument = *key;
				} else {
					croak_cb(cb, "Integer argument is required for arithmetic and delete operations");
				}

				*sz += 1 // mp_sizeof_array(3)
				     + 2 // mp_sizeof_str(1)
				     + mp_sizeof_uint(field_no)
				;

				sv_size_check(rv, h, *sz);

				h = mp_encode_array(h, 3);
				h = mp_encode_str(h, op, 1);
				h = mp_encode_uint(h, field_no);
				h = encode_obj(argument, h, rv, sz, field_format);

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

				*sz += 1 // mp_sizeof_array(5)
				     + 2 // mp_sizeof_str(1)
				     + mp_sizeof_uint(field_no)
				     + mp_sizeof_uint(position)
				     + mp_sizeof_uint(offset)
				;

				sv_size_check(rv, h, *sz);

				h = mp_encode_array(h, 5);
				h = mp_encode_str(h, op, 1);
				h = mp_encode_uint(h, field_no);
				h = mp_encode_uint(h, position);
				h = mp_encode_uint(h, offset);
				h = encode_obj(argument, h, rv, sz, FMT_STRING);

				break;
			}

			case OP_UPD_UNKNOWN: {
				croak_cb(cb, "Update operation is unknown");
			}
		}
	}

	return h;
}


static inline SV *pkt_update(TntCtx *ctx, uint32_t iid, HV *spaces, SV *space, SV *keys, SV *operations, HV *opt, SV *cb) {
	U32 index  = 0;

	unpack_format *fmt;
	dUnpackFormat(format);

	SV **key;
	TntSpace *spc = 0;
	TntIndex *idx = 0;

	if(( spc = evt_find_space( space, spaces, ctx->log_level, cb ) )) {
		ctx->space = spc;
	} else {
		ctx->use_hash = 0;
		return NULL;
	}

	if (opt) {
		if ((key = hv_fetch(opt, "index", 5, 0)) && SvOK(*key)) {
			if(( idx = evt_find_index( spc, key, ctx->log_level ) ))
				index = idx->id;
		}
		if ((key = hv_fetchs(opt, "hash", 0)) ) ctx->use_hash = SvOK(*key) ? SvIV( *key ) : 0;
	} else {
		ctx->f.size = 0;
	}

	if (!idx) {
		// Getting index #0
		if ( spc && spc->indexes && (key = hv_fetch( spc->indexes,(char *)&index,sizeof(U32),0 )) && *key) {
			idx = (TntIndex *) SvPVX(*key);
		}
	}
	evt_opt_in(opt, idx, key, format, fmt, cb);

	uint32_t body_map_sz = 3 + (index != -1);
	uint32_t keys_size = 0;


	size_t sz = HEADER_CONST_LEN
	          + 1 // mp_sizeof_map(body_map_sz)
	          + 1 // mp_sizeof_uint(TP_SPACE)
	          + mp_sizeof_uint(spc->id);

	if (index != -1) {
		sz += 1 // mp_sizeof_uint(TP_INDEX)
		    + mp_sizeof_uint(index);
	}

	sz += 1; // mp_sizeof_uint(TP_KEY);


	// counting fields in keys
	SV *t = keys;
	AV *fields;
	if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVHV) {
		fields = hash_to_array_fields( (HV *) SvRV(t), idx->fields, true, cb );
	} else if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVAV) {
		fields  = (AV *) SvRV(t);
	} else {
		if (!ctx->f.nofree) safefree(ctx->f.f);
		croak_cb(cb, "Input container is invalid. Expecting ARRAYREF or HASHREF");
	}

	keys_size = av_len(fields) + 1;
	sz += mp_sizeof_array(keys_size);

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

	h = pkt_update_write_operations(ctx, spc, idx, TP_TUPLE, operations, &sz, rv, h, cb);
	if (!h) return NULL;

	char *p = SvPVX(rv);
	write_length(p, h-p-5);
	SvCUR_set(rv, h-p);

	return SvREFCNT_inc(rv);
}


static inline SV *pkt_upsert(TntCtx *ctx, uint32_t iid, HV *spaces, SV *space, SV *tuple, SV *operations, HV *opt, SV *cb) {
	U32 index  = 0;

	unpack_format *fmt;
	dUnpackFormat(format);

	SV **key;
	TntSpace *spc = 0;
	TntIndex *idx = 0;

	if (( spc = evt_find_space( space, spaces, ctx->log_level, cb ) )) {
		ctx->space = spc;
	} else {
		ctx->use_hash = 0;
		return NULL;
	}

	if (opt) {
		if ((key = hv_fetchs(opt, "hash", 0)) ) ctx->use_hash = SvOK(*key) ? SvIV( *key ) : 0;
	} else {
		ctx->f.size = 0;
	}

	if (!idx) {
		if ( spc && spc->indexes && (key = hv_fetch(spc->indexes, (char *) &index, sizeof(U32), 0)) && *key) {
			idx = (TntIndex *) SvPVX(*key);
		}
	}
	evt_opt_in(opt, idx, key, format, fmt, cb);

	uint32_t body_map_sz = 3;  // space, tuple and operations
	uint32_t tuple_size = 0;


	size_t sz = HEADER_CONST_LEN
	          + 1 // mp_sizeof_map(body_map_sz)
	          + 1 // mp_sizeof_uint(TP_SPACE)
	          + mp_sizeof_uint(spc->id);

	sz += 1; // mp_sizeof_uint(TP_TUPLE);

	// counting fields in tuple
	SV *t = tuple;
	AV *fields;
	if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVHV) {
		fields = hash_to_array_fields( (HV *) SvRV(t), idx->fields, false, cb );
	} else if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVAV) {
		fields  = (AV *) SvRV(t);
	} else {
		if (!ctx->f.nofree) safefree(ctx->f.f);
		croak_cb(cb, "Input container is invalid. Expecting ARRAYREF or HASHREF");
	}

	tuple_size = av_len(fields) + 1;
	sz += mp_sizeof_array(tuple_size);

	create_buffer(rv, h, sz, TP_UPSERT, iid);
	h = mp_encode_map(h, body_map_sz);
	h = mp_encode_uint(h, TP_SPACE);
	h = mp_encode_uint(h, spc->id);

	h = mp_encode_uint(h, TP_TUPLE);
	h = mp_encode_array(h, tuple_size);
	encode_keys(h, sz, fields, tuple_size, fmt, key);

	h = pkt_update_write_operations(ctx, spc, idx, TP_OPERATIONS, operations, &sz, rv, h, cb);
	if (!h) return NULL;

	char *p = SvPVX(rv);
	write_length(p, h-p-5);
	SvCUR_set(rv, h-p);

	return SvREFCNT_inc(rv);
}


static inline SV *pkt_delete(TntCtx *ctx, uint32_t iid, HV *spaces, SV *space, SV *keys, HV *opt, SV *cb) {
	uint32_t index = 0;
	unpack_format *fmt;
	dUnpackFormat( format );

	SV **key;

	TntSpace *spc = 0;
	TntIndex *idx = 0;

	if (( spc = evt_find_space(space, spaces, ctx->log_level, cb) )) {
		ctx->space = spc;
	} else {
		ctx->use_hash = 0;
		return NULL;
	}

	if (opt) {
		if ((key = hv_fetch(opt, "index", 5, 0)) && SvOK(*key)) {
			if(( idx = evt_find_index( spc, key, ctx->log_level ) ))
				index = idx->id;
		}
		if ((key = hv_fetchs(opt, "hash", 0)) ) ctx->use_hash = SvOK(*key) ? SvIV( *key ) : 0;
	} else {
		ctx->f.size = 0;
	}
	if (!idx) {
		if ( spc && spc->indexes && (key = hv_fetch( spc->indexes,(char *)&index,sizeof(U32),0 )) && *key) {
			idx = (TntIndex *) SvPVX(*key);
		} else {
			log_warn(ctx->log_level, "No index %d config. Using without formats", index);
		}
	}
	evt_opt_in( opt, idx, key, format, fmt, cb );

	uint32_t body_map_sz = 2 + (index != -1);
	uint32_t keys_size = 0;

	size_t sz = HEADER_CONST_LEN
	          + 1 // mp_sizeof_map(body_map_sz)
	          + 1 // mp_sizeof_uint(TP_SPACE)
	          + mp_sizeof_uint(spc->id);

	if (index != -1) {
		sz += 1 // mp_sizeof_uint(TP_INDEX)
		    + mp_sizeof_uint(index);
	}

	sz += 1; // mp_sizeof_uint(TP_KEY);

	// counting fields in keys
	SV *t = keys;
	AV *fields;
	if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVHV) {
		fields = hash_to_array_fields( (HV *) SvRV(t), idx->fields, true, cb );
	} else if (SvROK(t) && SvTYPE(SvRV(t)) == SVt_PVAV) {
		fields  = (AV *) SvRV(t);
	} else {
		if (!ctx->f.nofree) safefree(ctx->f.f);
		croak_cb(cb, "Keys are invalid. Expecting ARRAYREF or HASHREF");
	}

	keys_size = av_len(fields) + 1;
	sz += mp_sizeof_array(keys_size);

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

static inline SV *pkt_eval(TntCtx *ctx, uint32_t iid, HV *spaces, SV *expression, SV *tuple, HV *opt, SV *cb) {
	unpack_format *fmt;
	dUnpackFormat(format);

	SV **key;
	TntSpace *spc = NULL;
	TntIndex *idx = NULL;

	if (opt) {
		if ((key = hv_fetchs(opt, "space", 0)) ) {
			if(( spc = evt_find_space( *key, spaces, ctx->log_level, cb ) )) {
				ctx->space = spc;
			} else {
				ctx->use_hash = 0;
				return NULL;
			}
		}
	} else {
		ctx->f.size = 0;
	}
	evt_opt_in(opt, idx, key, format, fmt, cb);

	uint32_t body_map_sz = 2;
	uint32_t keys_size = 0;

	if (unlikely(!SvPOK(expression))) {
		croak_cb(cb, "Eval expression must be a string");
	}

	uint32_t expression_size = SvCUR(expression);

	size_t sz = HEADER_CONST_LEN
	          + 1 // mp_sizeof_map(body_map_sz)
	          + 1 // mp_sizeof_uint(TP_EXPRESSION)
	          + mp_sizeof_str(expression_size)
	          + 1 // mp_sizeof_uint(TP_TUPLE)
	          ;

	if (unlikely( !tuple || !SvROK(tuple) || ( (SvTYPE(SvRV(tuple)) != SVt_PVAV) ))) {
		if (!ctx->f.nofree) safefree(ctx->f.f);
		croak_cb(cb, "Tuple is invalid. Expecting ARRAYREF");
	}

	AV *fields = (AV *) SvRV(tuple);

	keys_size = av_len(fields) + 1;
	sz += mp_sizeof_array(keys_size);

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

static inline SV *pkt_call(TntCtx *ctx, uint32_t iid, HV *spaces, SV *function_name, SV *tuple, HV *opt, SV *cb) {
	unpack_format *fmt;
	dUnpackFormat( format );

	SV **key;
	TntSpace *spc = NULL;
	TntIndex *idx = NULL;

	if (opt) {
		if ((key = hv_fetchs(opt, "space", 0)) ) {
			if(( spc = evt_find_space( *key, spaces, ctx->log_level, cb ) )) {
				ctx->space = spc;
			} else {
				ctx->use_hash = 0;
				return NULL;
			}
		}
	} else {
		ctx->f.size = 0;
	}
	evt_opt_in(opt, idx, key, format, fmt, cb);

	uint32_t body_map_sz = 2;
	uint32_t keys_size = 0;

	if (unlikely(!SvPOK(function_name))) {
		croak_cb(cb, "Function name must be a string");
	}

	uint32_t function_name_size = SvCUR(function_name);

	size_t sz = HEADER_CONST_LEN
	          + 1 // mp_sizeof_map(body_map_sz)
	          + 1 // mp_sizeof_uint(TP_FUNCTION)
	          + mp_sizeof_str(function_name_size)
	          + 1 // mp_sizeof_uint(TP_TUPLE)
	          ;

	if (unlikely( !tuple || !SvROK(tuple) || ( (SvTYPE(SvRV(tuple)) != SVt_PVAV) ))) {
		if (!ctx->f.nofree) safefree(ctx->f.f);
		croak_cb(cb, "Tuple is invalid. Expecting ARRAYREF");
	}

	AV *fields  = (AV *) SvRV(tuple);

	keys_size = av_len(fields) + 1;
	sz += mp_sizeof_array(keys_size);

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

static int parse_reply_hdr(HV *ret, const char *const data, STRLEN size, tnt_header_t *hdr, uint8_t log_level) {
	tnt_header_init(hdr);
	const char *p = data;
	const char *test = p;

	test = p;
	if (unlikely(mp_check(&test, data + size))) {
		cwarn("mp_check failed. mp_check(&test, data + size)");
		return -1;
	}
	if (unlikely(mp_typeof(*p) != MP_MAP)) {
		cwarn("typeof(*p) is not MP_MAP. It is %d", mp_typeof(*p));
		return -1;
	}

	uint32_t n = mp_decode_map(&p);
	while (n-- > 0) {
		if (unlikely(mp_typeof(*p) != MP_UINT)) {
			cwarn("typeof(*p) is not MP_UINT. It is %d", mp_typeof(*p));
			return -1;
		}

		uint32_t key = mp_decode_uint(&p);
		switch (key) {
			case TP_CODE:
				if (unlikely(mp_typeof(*p) != MP_UINT)) {
					cwarn("typeof(*p) is not MP_UINT. It is %d", mp_typeof(*p));
					return -1;
				}

				hdr->code = mp_decode_uint(&p);
				hdr->code &= 0x7FFF;
				break;

			case TP_SYNC:
				if (unlikely(mp_typeof(*p) != MP_UINT)) {
					cwarn("typeof(*p) is not MP_UINT. It is %d", mp_typeof(*p));
					return -1;
				}

				hdr->id = mp_decode_uint(&p);
				break;
			case TP_SCHEMA_ID:
				if (unlikely(mp_typeof(*p) != MP_UINT)) {
					cwarn("typeof(*p) is not MP_UINT. It is %d", mp_typeof(*p));
					return -1;
				}

				hdr->schema_id = mp_decode_uint(&p);
				break;
			default:
				log_info(log_level, "Got unknown tnt header key: %u. Skipping its value", key);
				mp_next(&p);
				break;
		}
	}

	(void) hv_stores(ret, "code", newSViv(hdr->code));
	(void) hv_stores(ret, "sync", newSViv(hdr->id));
	if (hdr->schema_id > 0) {
		(void) hv_stores(ret, "schema_id", newSViv(hdr->schema_id));
	}

	return p - data;
}


static inline int parse_reply_body_data(TntCtx *ctx, HV *ret, const char *const data_begin, const char *const data_end, const unpack_format *const format, AV *fields) {
	STRLEN data_size = data_end - data_begin;
	if (data_size == 0)
		return 0;

	const char *p = data_begin;

	uint32_t cont_size = 0;
	switch (mp_typeof(*p)) {
	case MP_ARRAY: {
		cont_size = mp_decode_array(&p);
		// cwarn("tuples count = %d", cont_size);

		AV *tuples = newAV();
		av_extend(tuples, cont_size);
		(void) hv_stores(ret, "count", newSViv(cont_size));
		(void) hv_stores(ret, "tuples", newRV_noinc((SV *) tuples));

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
					SV *field_value = decode_obj(&p);

					if (k < known_tuple_size && (name = av_fetch(fields, k, 0)) && *name) {
						(void) hv_store(tuple, SvPV_nolen(*name), sv_len(*name), field_value, 0);
					} else {
						// cwarn("Field name for field %d is not defined", k);
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
				AV *tuple = newAV();
				av_push(tuples, newRV_noinc((SV *)tuple));
				tuple_size = mp_decode_array(&p);
				av_extend(tuple, tuple_size);

				for (k = 0; k < tuple_size; ++k) {
					(void) av_push(tuple, decode_obj(&p));
				}
			}
		}

		break;
	}
	default:
		log_error(ctx->log_level, "unexpected response data type = %d", mp_typeof(*p));
		break;
	}

	return 0;
}

static inline tnt_format_t parse_format_string(const char *str, uint32_t str_len) {
	if ((str_len == 3 && strncasecmp(str, "num", 3) == 0)
		|| (str_len == 4 && strncasecmp(str, "uint", 4) == 0)
		|| (str_len == 8 && strncasecmp(str, "unsigned", 8) == 0)) {
		return FMT_UNSIGNED;
	}
	else
	if ((str_len == 3 && strncasecmp(str, "str", 3) == 0)
		|| (str_len == 6 && strncasecmp(str, "string", 6) == 0)) {
		return FMT_STRING;
	}
	else
	if (str_len == 6 && strncasecmp(str, "number", 6) == 0) {
		return FMT_NUMBER;
	}
	else
	if ((str_len == 3 && strncasecmp(str, "int", 3) == 0)
		|| (str_len == 7 && strncasecmp(str, "integer", 7) == 0)) {
		return FMT_INTEGER;
	}
	else
	if (str_len == 5 && strncasecmp(str, "array", 5) == 0) {
		return FMT_ARRAY;
	}
	else
	if (str_len == 6 && strncasecmp(str, "scalar", 6) == 0) {
		return FMT_SCALAR;
	}
	else
	if (str_len == 1 && strncasecmp(str, "*", 1) == 0) {
		return FMT_UNKNOWN;
	}
	else {
		return FMT_BAD;
	}
}

static inline int parse_spaces_body_data(HV *ret, const char *const data_begin, const char *const data_end, uint8_t log_level) {
	const uint32_t VALID_TUPLE_SIZE = 7;

	STRLEN data_size = data_end - data_begin;
	if (data_size == 0)
		return 0;

	const char *p = data_begin;

	uint32_t cont_size = 0;
	switch (mp_typeof(*p)) {
	case MP_ARRAY: {
		cont_size = mp_decode_array(&p);
		// cwarn("tuples count = %d", cont_size);

		HV *data = newHV();

		(void) hv_stores(ret, "count", newSViv(cont_size));
		(void) hv_stores(ret, "data", newRV_noinc((SV *) data));

		uint32_t tuple_size = 0;
		uint32_t i = 0, k;

		for (i = 0; i < cont_size; ++i) {
			k = 0;

			tuple_size = mp_decode_array(&p);
			// cwarn("tuple_size = %d", tuple_size);

			if (tuple_size < 1) {
				log_error(log_level, "Invalid tuple size. Should be %d. Got %d. Exiting", VALID_TUPLE_SIZE, tuple_size);
				return 0;
			}

			if (tuple_size != VALID_TUPLE_SIZE) {
				log_error(log_level, "Invalid tuple size. Should be %d. Got %d", VALID_TUPLE_SIZE, tuple_size);
			}

			SV *sv_id = sv_2mortal(decode_obj(&p));
			int id = (U32) SvUV(sv_id);
			// cwarn("space_id=%d", id);

			dSVX(spcf, spc, TntSpace);

			spc->id = id;
			spc->indexes = newHV();
			spc->field   = newHV();
			spc->fields  = newAV();

			spc->f.nofree = 1;
			spc->f.def = FMT_UNKNOWN;

			++k; if (k < tuple_size) spc->owner = decode_obj(&p);
			++k; if (k < tuple_size) spc->name = decode_obj(&p);
			++k; if (k < tuple_size) spc->engine = decode_obj(&p);
			++k; if (k < tuple_size) spc->fields_count = decode_obj(&p);
			++k; if (k < tuple_size) spc->flags = decode_obj(&p);
			++k; if (k < tuple_size) { // format

				uint32_t str_len = 0;
				uint32_t format_arr_size = mp_decode_array(&p);

				spc->f.size = format_arr_size;
				if (spc->f.size) {
					spc->f.f = safemalloc(spc->f.size + 1);
					spc->f.f[spc->f.size] = 0;
				}

				uint32_t ix;
				for (ix = 0; ix < format_arr_size; ++ix) {
					uint32_t field_format_map_size = mp_decode_map(&p);
					if (field_format_map_size != 2) {
						croak("Bad things happened! field_format_map_size != 2");
					}

					SV *field_name = NULL;
					uint32_t field_name_len = 0;
					while (field_format_map_size-- > 0) {

						const char *str = mp_decode_str(&p, &str_len);

						if (str_len == 4 && strncasecmp(str, "name", 4) == 0) {
							str = mp_decode_str(&p, &str_len); // getting the name itself
							field_name = sv_2mortal(newSVpvn(str, str_len));
							field_name_len = str_len;
						}
						else
						if (str_len == 4 && strncasecmp(str, "type", 4) == 0) {
							str = mp_decode_str(&p, &str_len); // getting the type itself
							
							tnt_format_t part_format = parse_format_string(str, str_len);
							if (part_format == FMT_BAD) {
								log_warn(log_level, "Unknown part %d type \'%.*s\' for space \'%.*s\'", ix, str_len, str, (int) SvCUR(spc->name), SvPV_nolen(spc->name));
								part_format = FMT_UNKNOWN;
							}
							spc->f.f[ix] = part_format;
						}
					}

					if (field_name == NULL) {
						log_warn(log_level, "field_name is NULL. Unexpected.");
						continue;
					}

					dSVX(fldsv, fld, TntField);
					fld->id = ix;
					fld->format = spc->f.f[ix];
					fld->name = field_name;
					(void) hv_store(spc->field, SvPV_nolen(field_name), field_name_len, fldsv, 0);
					// cwarn("field_name = %.*s", SvCUR(field_name), SvPV_nolen(field_name));
					av_push(spc->fields, SvREFCNT_inc_NN(field_name));
				}
			}

			(void) hv_store(data, (char *) &id, sizeof(U32), spcf, 0);
			(void) hv_store(data, SvPV_nolen(spc->name), SvCUR(spc->name), SvREFCNT_inc(spcf),0);
		}

		break;
	}
	default:
		log_error(log_level, "unexpected response data type (%d)", mp_typeof(*p));
		break;
	}

	return 0;
}

static inline int parse_index_body_data(HV *spaces, const char *const data_begin, const char *const data_end, uint8_t log_level) {
	STRLEN data_size = data_end - data_begin;
	if (data_size == 0)
		return 0;

	const char *p = data_begin;

	uint32_t cont_size = 0;
	switch (mp_typeof(*p)) {
	case MP_ARRAY: {
		cont_size = mp_decode_array(&p);
		// cwarn("tuples count = %d", cont_size);
		
		uint32_t i = 0;

		SV **key;
		for (i = 0; i < cont_size; ++i) {

			(void) mp_decode_array(&p);

			uint32_t space_id = mp_decode_uint(&p);
			uint32_t index_id = mp_decode_uint(&p);

			if ((key = hv_fetch(spaces, (char *) &space_id, sizeof(uint32_t), 0)) && *key) {
				TntSpace *spc = (TntSpace *) SvPVX(*key);

				dSVX(idxcf, idx, TntIndex);
				idx->id = index_id;
				
				if (mp_typeof(*p) != MP_STR) croak("[parse_index_body_data] index name has to be string");
				idx->name = decode_obj(&p);
				
				if (mp_typeof(*p) != MP_STR) croak("[parse_index_body_data] index type has to be string");
				idx->type = decode_obj(&p);
				
				if (mp_typeof(*p) != MP_MAP) croak("[parse_index_body_data] index opts has to be map");
				idx->opts = (HV *) decode_obj(&p);

				if (mp_typeof(*p) != MP_ARRAY) croak("[parse_index_body_data] index parts has to be array");
				uint32_t parts_count = mp_decode_array(&p);

				idx->f.nofree = 1;
				idx->f.size = parts_count;
				if (idx->f.size) {
					idx->f.f = safemalloc(idx->f.size + 1);
					idx->f.f[idx->f.size] = 0;
				}
				idx->f.def = FMT_UNKNOWN;
				idx->fields = newAV();
				av_extend(idx->fields, parts_count);

				uint32_t part_i;
				uint32_t ix = 0;
				const char *str;
				uint32_t str_len;

				for (part_i = 0; part_i < parts_count; ++part_i) {
					uint32_t index_part_size = mp_decode_array(&p);
					
					ix = mp_decode_uint(&p);
					str = mp_decode_str(&p, &str_len);
					if (index_part_size == 3) {
						mp_next(&p); // TODO: index part options
					}
					
					tnt_format_t part_format = parse_format_string(str, str_len);
					if (part_format == FMT_BAD) {
						log_info(log_level, "Unknown part type \'%.*s\' for index %d of space \'%.*s\'", str_len, str, index_id, (int) SvCUR(spc->name), SvPV_nolen(spc->name));
						part_format = FMT_UNKNOWN;
					}
					idx->f.f[part_i] = part_format;

					SV **f = av_fetch(spc->fields, ix, 0);
					if (f) {
						HE *fhe = hv_fetch_ent(spc->field, *f, 1, 0);
						if (SvOK( HeVAL(fhe) )) {
							av_push(idx->fields, SvREFCNT_inc_NN(((TntField *)SvPVX( HeVAL(fhe) ))->name));
							// idx->f.f[ix] = ((TntField *)SvPVX( HeVAL(fhe) ))->format;
						}
					} else {
						log_info(log_level, "The field %d of space %.*s is not in space format information", ix, (int) SvCUR(spc->name), SvPV_nolen(spc->name));
					}
				}


				(void) hv_store(spc->indexes, (char *) &index_id, sizeof(uint32_t), idxcf, 0);
				(void) hv_store(spc->indexes, SvPV_nolen(idx->name), SvCUR(idx->name), SvREFCNT_inc(idxcf), 0);

			} else {
				log_info(log_level, "Space definition not found for space %d", space_id);
			}
		}

		break;
	}
	default:
		log_warn(log_level, "unexpected response data type = %d", mp_typeof(*p));
		break;
	}

	return 0;
}

static int parse_reply_body(TntCtx *ctx, HV *ret, const char *const data, STRLEN size, const unpack_format *const format, AV *fields) {
	const char *p = data;
	const char *test = p;
	// body
	if (p == data + size) {
		return size;
	}

	test = p;
	if (unlikely(mp_check(&test, data + size))) {
		cwarn("mp_check failed: mp_check(&test, data + size).");
		return -1;
	}
	if (unlikely(mp_typeof(*p) != MP_MAP)) {
		cwarn("typeof(*p) is not MP_MAP. It is %d", mp_typeof(*p));
		return -1;
	}
	int n = mp_decode_map(&p);
	while (n-- > 0) {
		uint32_t key = mp_decode_uint(&p);
		switch (key) {
		case TP_ERROR: {
			if (unlikely(mp_typeof(*p) != MP_STR)) {
				cwarn("typeof(*p) is not MP_STR in TP_ERROR");
				return -1;
			}
			uint32_t elen = 0;
			const char *err_str = mp_decode_str(&p, &elen);

			(void) hv_stores(ret, "status", newSVpvs("error"));
			(void) hv_stores(ret, "errstr", newSVpvn(err_str, elen));
			break;
		}

		case TP_DATA: {
			if (unlikely(mp_typeof(*p) != MP_ARRAY)) {
				cwarn("typeof(*p) is not MP_ARRAY in TP_DATA");
				return -1;
			}

			(void) hv_stores(ret, "status", newSVpvs("ok"));
			const char *data_begin = p;
			mp_next(&p);
			parse_reply_body_data(ctx, ret, data_begin, p, format, fields);
			break;
		}
		
		default: {
			cwarn("Got unknown tnt reply body key: %u. Skipping its value", key);
			break;
		}
		}
	}
	return p - data;
}


static int parse_spaces_body(HV *ret, const char *const data, STRLEN size, uint8_t log_level) {
	const char *p = data;
	const char *test = p;
	// body
	if (p == data + size) {
		return size;
	}

	test = p;
	if (unlikely(mp_check(&test, data + size))) {
		cwarn("mp_check failed: mp_check(&test, data + size).");
		return -1;
	}
	if (unlikely(mp_typeof(*p) != MP_MAP)) {
		cwarn("typeof(*p) is not MP_MAP. It is %d", mp_typeof(*p));
		return -1;
	}
	int n = mp_decode_map(&p);
	while (n-- > 0) {
		uint32_t key = mp_decode_uint(&p);
		switch (key) {
		case TP_ERROR: {
			if (unlikely(mp_typeof(*p) != MP_STR)) {
				cwarn("typeof(*p) is not MP_STR in TP_ERROR");
				return -1;
			}
			uint32_t elen = 0;
			const char *err_str = mp_decode_str(&p, &elen);

			(void) hv_stores(ret, "status", newSVpvs("error"));
			(void) hv_stores(ret, "errstr", newSVpvn(err_str, elen));
			break;
		}

		case TP_DATA: {
			if (unlikely(mp_typeof(*p) != MP_ARRAY)) {
				cwarn("typeof(*p) is not MP_ARRAY in TP_DATA");
				return -1;
			}

			(void) hv_stores(ret, "status", newSVpvs("ok"));
			const char *data_begin = p;
			mp_next(&p);
			parse_spaces_body_data(ret, data_begin, p, log_level);
			break;
		}
		default: {
			cwarn("Got unknown tnt reply body key: %u. Skipping its value", key);
			break;
		}
		}
	}
	return p - data;
}

static int parse_index_body(HV *spaces, HV *err_ret, const char *const data, STRLEN size, uint8_t log_level) {
	const char *p = data;
	const char *test = p;
	// body
	if (p == data + size) {
		return size;
	}

	test = p;
	if (unlikely(mp_check(&test, data + size))) {
		cwarn("mp_check failed: mp_check(&test, data + size).");
		return -1;
	}
	if (unlikely(mp_typeof(*p) != MP_MAP)) {
		cwarn("typeof(*p) is not MP_MAP. It is %d", mp_typeof(*p));
		return -1;
	}
	int n = mp_decode_map(&p);
	while (n-- > 0) {
		uint32_t key = mp_decode_uint(&p);
		switch (key) {
		case TP_ERROR: {
			if (unlikely(mp_typeof(*p) != MP_STR)) {
				cwarn("typeof(*p) is not MP_STR in TP_ERROR");
				return -1;
			}
			uint32_t elen = 0;
			const char *err_str = mp_decode_str(&p, &elen);
			(void) hv_stores(err_ret, "status", newSVpvs("error"));
			(void) hv_stores(err_ret, "errstr", newSVpvn(err_str, elen));
			break;
		}

		case TP_DATA: {
			if (unlikely(mp_typeof(*p) != MP_ARRAY)) {
				cwarn("typeof(*p) is not MP_ARRAY in TP_DATA");
				return -1;
			}

			const char *data_begin = p;
			mp_next(&p);
			parse_index_body_data(spaces, data_begin, p, log_level);


			break;
		}
		default: {
			cwarn("Got unknown tnt reply body key: %u. Skipping its value", key);
			break;
		}
		}
	}
	return p - data;
}

#endif // _XSTNT16_H_
