#ifndef _ENCDEC_H_
#define _ENCDEC_H_

#include "types.h"

#define TNT_GREET_LENGTH 128
#define TNT_VER_LENGTH 64
#define TNT_SALT_LENGTH 44
#define TNT_PAD_LENGTH ((TNT_GREET_LENGTH) - (TNT_VER_LENGTH) - (TNT_SALT_LENGTH))

static HV *types_boolean_stash;
static SV *types_true, *types_false;

#define write_length(h, size) STMT_START {  \
	*h = 0xce;								\
	*((uint32_t *)(h+1)) = htobe32(size);	\
} STMT_END

#define write_iid(h, iid) STMT_START {  \
	*h = 0xce;                          \
	*(uint32_t*)(h + 1) = htobe32(iid); \
	h += 5;                             \
} STMT_END


#define create_buffer(NAME, P_NAME, sz, tp_operation, iid)              \
	SV *NAME = sv_2mortal(newSV((sz)));                                 \
	SvUPGRADE(NAME, SVt_PV);                                            \
	SvPOK_on(NAME);                                                     \
																		\
	char *P_NAME = (char *) SvPVX(NAME);                                \
	P_NAME = mp_encode_map(P_NAME + 5, 2);                              \
	P_NAME = mp_encode_uint(P_NAME, TP_CODE);                           \
	P_NAME = mp_encode_uint(P_NAME, (tp_operation));                    \
	P_NAME = mp_encode_uint(P_NAME, TP_SYNC);                           \
	write_iid(P_NAME, (iid));                                           \

#define sv_size_check(svx, svx_end, totalneed)      \
	STMT_START {                                    \
		if ( totalneed < SvLEN(svx) )  {            \
		}                                           \
		else {                                      \
			STRLEN used = svx_end - SvPVX(svx);     \
			svx_end = sv_grow(svx, totalneed);      \
			svx_end += used;                        \
		}                                           \
	} STMT_END

#define encode_keys(h, sz, fields, keys_size, fmt, key) STMT_START {    \
	uint8_t field_max_size = 0;                                         \
	for (k = 0; k < keys_size; k++) {                                   \
		key = av_fetch( fields, k, 0 );                                 \
		if (key && *key && SvOK(*key) && sv_len(*key)) {                \
			char _fmt = k < fmt->size ? fmt->f[k] : fmt->def;           \
			h = encode_obj(*key, h, rv, &sz, _fmt);                     \
		} else {                                                        \
			h = encode_obj(&PL_sv_undef, h, rv, &sz, FMT_UNKNOWN);		\
			cwarn("Passed key is invalid. Consider revising.");			\
		}                                                               \
	}                                                                   \
} STMT_END

#define encode_str(dest, sz, rv, str, str_len) STMT_START { 	\
	*sz += mp_sizeof_str(str_len);								\
	sv_size_check(rv, dest, *sz);								\
	dest = mp_encode_str(dest, str, str_len);					\
} STMT_END

#define encode_double(dest, sz, rv, v) STMT_START { 	\
	*sz += mp_sizeof_double(v);							\
	sv_size_check(rv, dest, *sz);						\
	dest = mp_encode_double(dest, v);					\
} STMT_END

#define encode_uint(dest, sz, rv, v) STMT_START { 	\
	*sz += mp_sizeof_uint(v);						\
	sv_size_check(rv, dest, *sz);					\
	dest = mp_encode_uint(dest, v);					\
} STMT_END

#define encode_int(dest, sz, rv, v) STMT_START { 	\
	*sz += mp_sizeof_int(v);						\
	sv_size_check(rv, dest, *sz);					\
	dest = mp_encode_int(dest, v);					\
} STMT_END

#define encode_bool(dest, sz, rv, v) STMT_START { 	\
	*sz += 1; 										\
	sv_size_check(rv, dest, *sz);					\
	dest = mp_encode_bool(dest, v);					\
} STMT_END

#define encode_nil(dest, sz, rv) STMT_START {	 	\
	*sz += 1; 										\
	sv_size_check(rv, dest, *sz);					\
	dest = mp_encode_nil(dest);						\
} STMT_END

#define encode_array(dest, sz, rv, arr_size) STMT_START {	\
	*sz += mp_sizeof_array(arr_size);						\
	sv_size_check(rv, dest, *sz);							\
	dest = mp_encode_array(dest, arr_size);					\
} STMT_END

#define encode_map(dest, sz, rv, keys_size) STMT_START {	\
	*sz += mp_sizeof_map(keys_size);						\
	sv_size_check(rv, dest, *sz);							\
	dest = mp_encode_map(dest, keys_size);					\
} STMT_END

#define REAL_SV(sv, real_sv, stash)\
	HV *stash = NULL;\
	SV *real_sv = NULL;\
	if (SvROK(src)) {\
		real_sv = SvRV(sv);\
		if (SvOBJECT(real_sv)) {\
			stash = SvSTASH(real_sv);\
		}\
	} else {\
		real_sv = sv;\
	}


#define encode_AV(actual_src, rv, dest, sz) STMT_START {\
	AV *arr = (AV *) actual_src;\
	uint32_t arr_size = av_len(arr) + 1;\
	uint32_t i = 0;\
	encode_array(dest, sz, rv, arr_size);\
	\
	SV **elem;\
	for (i = 0; i < arr_size; ++i) {\
		elem = av_fetch(arr, i, 0);\
		if (elem && *elem && SvTYPE(*elem) != SVt_NULL) {\
			dest = encode_obj(*elem, dest, rv, sz, FMT_UNKNOWN);\
		} else {\
			encode_nil(dest, sz, rv);\
		}\
	}\
} STMT_END



static char *encode_obj(SV *src, char *dest, SV *rv, size_t *sz, char fmt) {
	// cwarn("fmt = %d", fmt);

	SvGETMAGIC(src);

	if (fmt == FMT_STR) {
		REAL_SV(src, actual_src, stash);

		STRLEN str_len = 0;
		char *str = NULL;

		if (SvPOK(actual_src)) {
			str = SvPV_nolen(actual_src);
			str_len = SvCUR(actual_src);
		} else {
			str = SvPV(actual_src, str_len);
			str_len = SvCUR(actual_src);
		}

		encode_str(dest, sz, rv, str, str_len);
		return dest;

	} else if (fmt == FMT_NUMBER || fmt == FMT_NUM || fmt == FMT_INT)  {

		if (fmt == FMT_NUMBER) {
			if (SvNOK(src)) {
				encode_double(dest, sz, rv, SvNVX(src));
				return dest;
			}
		}

		if (SvIOK(src)) {
			if (SvUOK(src)) {
				encode_uint(dest, sz, rv, SvUVX(src));
				return dest;
			} else {
				IV num = SvIVX(src);
				if (num >= 0) {
					encode_uint(dest, sz, rv, num);
					return dest;
				} else {
					encode_int(dest, sz, rv, num);
					return dest;
				}
			}
		} else if (SvPOK(src)) {
			if (fmt == FMT_NUMBER) {
				encode_double(dest, sz, rv, SvNV(src));
				return dest;
			} else {
				NV num = SvNV(src);
				if (SvUOK(src)) {
					encode_uint(dest, sz, rv, SvUV(src));
					return dest;
				} else {
					if (num >= 0) {
						encode_uint(dest, sz, rv, SvIV(src));
						return dest;
					} else {
						encode_int(dest, sz, rv, SvIV(src));
						return dest;
					}
				}
			}
		} else {
			croak("Incompatible types. Format expects: %c", fmt);
		}

	} else if (fmt == FMT_ARRAY) {
		REAL_SV(src, actual_src, stash);

		if (SvTYPE(actual_src) == SVt_PVAV) {
			encode_AV(actual_src, rv, dest, sz);
			return dest;
		} else {
			croak("Incompatible types. Format expects: %c", fmt);
		}

	} else if (fmt == FMT_UNKNOWN) {

		HV *boolean_stash = types_boolean_stash ? types_boolean_stash : gv_stashpv ("Types::Serialiser::Boolean", 1);
		REAL_SV(src, actual_src, stash);

		if (stash == boolean_stash) {
			bool v = (bool) SvIV(actual_src);
			encode_bool(dest, sz, rv, v);
			return dest;
		} else {

			if (SvTYPE(actual_src) == SVt_NULL) {
				encode_nil(dest, sz, rv);
				return dest;

			} else if (SvTYPE(actual_src) == SVt_PVAV) {  // array

				encode_AV(actual_src, rv, dest, sz);
				return dest;

			} else if (SvTYPE(actual_src) == SVt_PVHV) {  // hash

				HV *hv = (HV *) actual_src;
				HE *he;

				uint32_t keys_size = hv_iterinit(hv);

				encode_map(dest, sz, rv, keys_size);
				STRLEN nlen;
				while ((he = hv_iternext(hv))) {
					char *name = HePV(he, nlen);
					encode_str(dest, sz, rv, name, nlen);
					dest = encode_obj(HeVAL(he), dest, rv, sz, FMT_UNKNOWN);
				}
				return dest;

			} else if (SvNOK(actual_src)) {  // double
				encode_double(dest, sz, rv, SvNVX(actual_src));
				return dest;
			} else if (SvUOK(actual_src)) {  // uint
				encode_uint(dest, sz, rv, SvUVX(actual_src));
				return dest;

			} else if (SvIOK(actual_src)) {  // int or uint
				IV num = SvIVX(src);
				if (num >= 0) {
					encode_uint(dest, sz, rv, num);
					return dest;
				} else {
					encode_int(dest, sz, rv, num);
					return dest;
				}
			} else if (SvPOK(actual_src)) {  // string
				encode_str(dest, sz, rv, SvPV_nolen(actual_src), SvCUR(actual_src));
				return dest;
			} else {
				croak("What the heck is that?");
			}
		}

	} else {
		croak("Not implemented");
	}

	return dest;
}


#define decode_greeting(data, tnt_ver_begin, tnt_ver_end, salt_begin, salt_end) STMT_START {\
	char *p = data;\
	tnt_ver_begin = p;\
	p += TNT_VER_LENGTH;\
	tnt_ver_end = p;\
	\
	salt_begin = p;\
	p += TNT_SALT_LENGTH;\
	salt_end = p;\
	\
	p += TNT_PAD_LENGTH;\
} STMT_END

#define decode_pkt_len_(h, out) STMT_START {\
	char *p = *h;\
	uint32_t l = *((uint32_t *)(p+1));\
	*h += 5;\
	out = be32toh(l);\
} STMT_END

static inline uint32_t decode_pkt_len(char **h) {
	char *p = *h;
	uint32_t l = *((uint32_t *)(p+1));
	*h += 5;
	return be32toh(l);
}


static SV *decode_obj(const char **p) {
	uint32_t i = 0;
	const char *str = NULL;
	uint32_t str_len = 0;

	switch (mp_typeof(**p)) {
	case MP_UINT: {
		uint64_t value = mp_decode_uint(p);
		return (SV *) newSVuv(value);
	}
	case MP_INT: {
		int64_t value = mp_decode_int(p);
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
			av_push(arr, decode_obj(p));
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
			switch(mp_typeof(**p)) {
			case MP_STR: {
				map_key_str = mp_decode_str(p, &map_key_len);
				break;
			}
			case MP_UINT: {
				uint64_t value = mp_decode_uint(p);
				SV *s = sv_2mortal(newSVuv(value));
				STRLEN l;

				map_key_str = SvPV(s, l);
				map_key_len = (uint32_t) l;
				break;
			}
			case MP_INT: {
				int64_t value = mp_decode_int(p);
				SV *s = sv_2mortal(newSViv(value));
				STRLEN l;

				map_key_str = SvPV(s, l);
				map_key_len = (uint32_t) l;
				break;
			}
			default:
				cwarn("Got unexpected type as a tuple map key");
				_set = false;
				break;
			}
			if (_set) {
				SV *value = decode_obj(p);
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
		cwarn("Got unexpected type as a tuple element value");
		mp_next(p);
		return &PL_sv_undef;
	}
}


#endif // _ENCDEC_H_
