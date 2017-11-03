#ifndef XSENDIAN_H
#define XSENDIAN_H

#ifndef le64toh

#ifdef __APPLE__
#  include <libkern/OSByteOrder.h>
#  define bswap_16(x) OSSwapInt16(x)
#  define bswap_32(x) OSSwapInt32(x)
#  define bswap_64(x) OSSwapInt64(x)
#elif __FreeBSD__
#  include <sys/endian.h>
#  define bswap_16(x) bswap16(x)
#  define bswap_32(x) bswap32(x)
#  define bswap_64(x) bswap64(x)
#else
#  include <byteswap.h>
#  include <endian.h>
#endif /* __APPLE__ */


# if __BYTE_ORDER == __LITTLE_ENDIAN

#ifndef le16toh
#  define htobe16(x) bswap_16 (x)
#  define htole16(x) (x)
#  define be16toh(x) bswap_16 (x)
#  define le16toh(x) (x)
#endif

#ifndef le32toh
#  define htobe32(x) bswap_32 (x)
#  define htole32(x) (x)
#  define be32toh(x) bswap_32 (x)
#  define le32toh(x) (x)
#endif

#ifndef le64toh
#  define htobe64(x) bswap_64 (x)
#  define htole64(x) (x)
#  define be64toh(x) bswap_64 (x)
#  define le64toh(x) (x)
#endif

# else /* __BYTE_ORDER != __LITTLE_ENDIAN */

#ifndef le16toh
#  define htobe16(x) (x)
#  define htole16(x) bswap_16 (x)
#  define be16toh(x) (x)
#  define le16toh(x) bswap_16 (x)
#endif

#ifndef le32toh
#  define htobe32(x) (x)
#  define htole32(x) bswap_32 (x)
#  define be32toh(x) (x)
#  define le32toh(x) bswap_32 (x)
#endif

#ifndef le64toh
#  define htobe64(x) (x)
#  define htole64(x) bswap_64 (x)
#  define be64toh(x) (x)
#  define le64toh(x) bswap_64 (x)
#endif

#endif /* __BYTE_ORDER */

#endif /* le64toh */

#endif /* XSENDIAN_H */
