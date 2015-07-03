#ifndef XSMY_H
#define XSMY_H

#include "xsendian.h"
#include "log.h"
#include <stdio.h>

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

#ifndef cwarn
#define cwarn(fmt, ...)   do{ \
	fprintf(stderr, "[WARN] %s:%d: ", __FILE__, __LINE__); \
	fprintf(stderr, fmt, ##__VA_ARGS__); \
	if (fmt[strlen(fmt) - 1] != 0x0a) { fprintf(stderr, "\n"); } \
	} while(0)
#endif

#ifndef likely
#define likely(x) __builtin_expect((x),1)
#define unlikely(x) __builtin_expect((x),0)
#endif

#define dSVX(sv,ref,type) \
	SV *sv = newSV( sizeof(type) );\
	SvUPGRADE( sv, SVt_PV ); \
	SvCUR_set(sv,sizeof(type)); \
	SvPOKp_on(sv); \
	type * ref = (type *) SvPVX( sv ); \
	memset(ref,0,sizeof(type)); \

#ifndef dObjBy
#define dObjBy(Type,obj,ptr,xx) Type * obj = (Type *) ( (char *) ptr - (ptrdiff_t) &((Type *) 0)-> xx )
#endif

void * safecpy(const void *src,register size_t len) {
	char *new = safemalloc(len+1);
	memcpy(new,src,len+1);
	new[len]=0;
	return new;
}

#define _croak_cb(cb,...) STMT_START {\
		warn(__VA_ARGS__);\
		if (cb) {\
			dSP;\
			ENTER;\
			SAVETMPS;\
			PUSHMARK(SP);\
			EXTEND(SP, 2);\
			PUSHs(&PL_sv_undef);\
			PUSHs( sv_2mortal(newSVpvf(__VA_ARGS__)) );\
			PUTBACK;\
			call_sv( cb, G_DISCARD | G_VOID );\
			FREETMPS;\
			LEAVE;\
		} else {\
			croak(__VA_ARGS__);\
		}\
} STMT_END

#define croak_cb(cb,...) STMT_START {\
		_croak_cb(cb, __VA_ARGS__);\
		return NULL;\
} STMT_END

#define croak_cb_void(cb,...) STMT_START {\
		_croak_cb(cb, __VA_ARGS__);\
		return;\
} STMT_END

#endif
