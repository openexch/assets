/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * Markets, assets and the behaviour version as a logged command (EngineConfig pattern). Decision table: recorded+equal = DUPLICATE, recorded+different = REJECTED, fresh = record, adopt = cross-check compiled tables and fail fast on mismatch. The assets group may change only while no account exists.
 */
@SuppressWarnings("all")
public final class CoreConfigEncoder
{
    public static final int BLOCK_LENGTH = 24;
    public static final int TEMPLATE_ID = 70;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final CoreConfigEncoder parentMessage = this;
    private MutableDirectBuffer buffer;
    private int offset;
    private int limit;

    public int sbeBlockLength()
    {
        return BLOCK_LENGTH;
    }

    public int sbeTemplateId()
    {
        return TEMPLATE_ID;
    }

    public int sbeSchemaId()
    {
        return SCHEMA_ID;
    }

    public int sbeSchemaVersion()
    {
        return SCHEMA_VERSION;
    }

    public String sbeSemanticType()
    {
        return "";
    }

    public MutableDirectBuffer buffer()
    {
        return buffer;
    }

    public int offset()
    {
        return offset;
    }

    public CoreConfigEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public CoreConfigEncoder wrapAndApplyHeader(
        final MutableDirectBuffer buffer, final int offset, final MessageHeaderEncoder headerEncoder)
    {
        headerEncoder
            .wrap(buffer, offset)
            .blockLength(BLOCK_LENGTH)
            .templateId(TEMPLATE_ID)
            .schemaId(SCHEMA_ID)
            .version(SCHEMA_VERSION);

        return wrap(buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH);
    }

    public int encodedLength()
    {
        return limit - offset;
    }

    public int limit()
    {
        return limit;
    }

    public void limit(final int limit)
    {
        this.limit = limit;
    }

    public static int correlationIdId()
    {
        return 1;
    }

    public static int correlationIdSinceVersion()
    {
        return 0;
    }

    public static int correlationIdEncodingOffset()
    {
        return 0;
    }

    public static int correlationIdEncodingLength()
    {
        return 8;
    }

    public static String correlationIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long correlationIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long correlationIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long correlationIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public CoreConfigEncoder correlationId(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
    }


    public static int configVersionId()
    {
        return 2;
    }

    public static int configVersionSinceVersion()
    {
        return 0;
    }

    public static int configVersionEncodingOffset()
    {
        return 8;
    }

    public static int configVersionEncodingLength()
    {
        return 4;
    }

    public static String configVersionMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long configVersionNullValue()
    {
        return 4294967295L;
    }

    public static long configVersionMinValue()
    {
        return 0L;
    }

    public static long configVersionMaxValue()
    {
        return 4294967294L;
    }

    public CoreConfigEncoder configVersion(final long value)
    {
        buffer.putInt(offset + 8, (int)value, BYTE_ORDER);
        return this;
    }


    public static int behaviorVersionId()
    {
        return 3;
    }

    public static int behaviorVersionSinceVersion()
    {
        return 0;
    }

    public static int behaviorVersionEncodingOffset()
    {
        return 12;
    }

    public static int behaviorVersionEncodingLength()
    {
        return 4;
    }

    public static String behaviorVersionMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long behaviorVersionNullValue()
    {
        return 4294967295L;
    }

    public static long behaviorVersionMinValue()
    {
        return 0L;
    }

    public static long behaviorVersionMaxValue()
    {
        return 4294967294L;
    }

    public CoreConfigEncoder behaviorVersion(final long value)
    {
        buffer.putInt(offset + 12, (int)value, BYTE_ORDER);
        return this;
    }


    public static int genesisIdId()
    {
        return 4;
    }

    public static int genesisIdSinceVersion()
    {
        return 7;
    }

    public static int genesisIdEncodingOffset()
    {
        return 16;
    }

    public static int genesisIdEncodingLength()
    {
        return 8;
    }

    public static String genesisIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long genesisIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long genesisIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long genesisIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public CoreConfigEncoder genesisId(final long value)
    {
        buffer.putLong(offset + 16, value, BYTE_ORDER);
        return this;
    }


    private final MarketsEncoder markets = new MarketsEncoder(this);

    public static long marketsId()
    {
        return 10;
    }

    public MarketsEncoder marketsCount(final int count)
    {
        markets.wrap(buffer, count);
        return markets;
    }

    public static final class MarketsEncoder
    {
        public static final int HEADER_SIZE = 4;
        private final CoreConfigEncoder parentMessage;
        private MutableDirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int initialLimit;

        MarketsEncoder(final CoreConfigEncoder parentMessage)
        {
            this.parentMessage = parentMessage;
        }

        public void wrap(final MutableDirectBuffer buffer, final int count)
        {
            if (count < 0 || count > 65534)
            {
                throw new IllegalArgumentException("count outside allowed range: count=" + count);
            }

            if (buffer != this.buffer)
            {
                this.buffer = buffer;
            }

            index = 0;
            this.count = count;
            final int limit = parentMessage.limit();
            initialLimit = limit;
            parentMessage.limit(limit + HEADER_SIZE);
            buffer.putShort(limit + 0, (short)60, BYTE_ORDER);
            buffer.putShort(limit + 2, (short)count, BYTE_ORDER);
        }

        public MarketsEncoder next()
        {
            if (index >= count)
            {
                throw new java.util.NoSuchElementException();
            }

            offset = parentMessage.limit();
            parentMessage.limit(offset + sbeBlockLength());
            ++index;

            return this;
        }

        public int resetCountToIndex()
        {
            count = index;
            buffer.putShort(initialLimit + 2, (short)count, BYTE_ORDER);

            return count;
        }

        public static int countMinValue()
        {
            return 0;
        }

        public static int countMaxValue()
        {
            return 65534;
        }

        public static int sbeHeaderSize()
        {
            return HEADER_SIZE;
        }

        public static int sbeBlockLength()
        {
            return 60;
        }

        public static int marketIdId()
        {
            return 1;
        }

        public static int marketIdSinceVersion()
        {
            return 0;
        }

        public static int marketIdEncodingOffset()
        {
            return 0;
        }

        public static int marketIdEncodingLength()
        {
            return 4;
        }

        public static String marketIdMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static int marketIdNullValue()
        {
            return -2147483648;
        }

        public static int marketIdMinValue()
        {
            return -2147483647;
        }

        public static int marketIdMaxValue()
        {
            return 2147483647;
        }

        public MarketsEncoder marketId(final int value)
        {
            buffer.putInt(offset + 0, value, BYTE_ORDER);
            return this;
        }


        public static int symbolId()
        {
            return 2;
        }

        public static int symbolSinceVersion()
        {
            return 0;
        }

        public static int symbolEncodingOffset()
        {
            return 4;
        }

        public static int symbolEncodingLength()
        {
            return 16;
        }

        public static String symbolMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static byte symbolNullValue()
        {
            return (byte)0;
        }

        public static byte symbolMinValue()
        {
            return (byte)32;
        }

        public static byte symbolMaxValue()
        {
            return (byte)126;
        }

        public static int symbolLength()
        {
            return 16;
        }


        public MarketsEncoder symbol(final int index, final byte value)
        {
            if (index < 0 || index >= 16)
            {
                throw new IndexOutOfBoundsException("index out of range: index=" + index);
            }

            final int pos = offset + 4 + (index * 1);
            buffer.putByte(pos, value);

            return this;
        }

        public static String symbolCharacterEncoding()
        {
            return java.nio.charset.StandardCharsets.US_ASCII.name();
        }

        public MarketsEncoder putSymbol(final byte[] src, final int srcOffset)
        {
            final int length = 16;
            if (srcOffset < 0 || srcOffset > (src.length - length))
            {
                throw new IndexOutOfBoundsException("Copy will go out of range: offset=" + srcOffset);
            }

            buffer.putBytes(offset + 4, src, srcOffset, length);

            return this;
        }

        public MarketsEncoder symbol(final String src)
        {
            final int length = 16;
            final int srcLength = null == src ? 0 : src.length();
            if (srcLength > length)
            {
                throw new IndexOutOfBoundsException("String too large for copy: byte length=" + srcLength);
            }

            buffer.putStringWithoutLengthAscii(offset + 4, src);

            for (int start = srcLength; start < length; ++start)
            {
                buffer.putByte(offset + 4 + start, (byte)0);
            }

            return this;
        }

        public MarketsEncoder symbol(final CharSequence src)
        {
            final int length = 16;
            final int srcLength = null == src ? 0 : src.length();
            if (srcLength > length)
            {
                throw new IndexOutOfBoundsException("CharSequence too large for copy: byte length=" + srcLength);
            }

            buffer.putStringWithoutLengthAscii(offset + 4, src);

            for (int start = srcLength; start < length; ++start)
            {
                buffer.putByte(offset + 4 + start, (byte)0);
            }

            return this;
        }

        public static int baseAssetIdId()
        {
            return 3;
        }

        public static int baseAssetIdSinceVersion()
        {
            return 0;
        }

        public static int baseAssetIdEncodingOffset()
        {
            return 20;
        }

        public static int baseAssetIdEncodingLength()
        {
            return 4;
        }

        public static String baseAssetIdMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static int baseAssetIdNullValue()
        {
            return -2147483648;
        }

        public static int baseAssetIdMinValue()
        {
            return -2147483647;
        }

        public static int baseAssetIdMaxValue()
        {
            return 2147483647;
        }

        public MarketsEncoder baseAssetId(final int value)
        {
            buffer.putInt(offset + 20, value, BYTE_ORDER);
            return this;
        }


        public static int quoteAssetIdId()
        {
            return 4;
        }

        public static int quoteAssetIdSinceVersion()
        {
            return 0;
        }

        public static int quoteAssetIdEncodingOffset()
        {
            return 24;
        }

        public static int quoteAssetIdEncodingLength()
        {
            return 4;
        }

        public static String quoteAssetIdMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static int quoteAssetIdNullValue()
        {
            return -2147483648;
        }

        public static int quoteAssetIdMinValue()
        {
            return -2147483647;
        }

        public static int quoteAssetIdMaxValue()
        {
            return 2147483647;
        }

        public MarketsEncoder quoteAssetId(final int value)
        {
            buffer.putInt(offset + 24, value, BYTE_ORDER);
            return this;
        }


        public static int tickSizeId()
        {
            return 5;
        }

        public static int tickSizeSinceVersion()
        {
            return 0;
        }

        public static int tickSizeEncodingOffset()
        {
            return 28;
        }

        public static int tickSizeEncodingLength()
        {
            return 8;
        }

        public static String tickSizeMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long tickSizeNullValue()
        {
            return -9223372036854775808L;
        }

        public static long tickSizeMinValue()
        {
            return -9223372036854775807L;
        }

        public static long tickSizeMaxValue()
        {
            return 9223372036854775807L;
        }

        public MarketsEncoder tickSize(final long value)
        {
            buffer.putLong(offset + 28, value, BYTE_ORDER);
            return this;
        }


        public static int minPriceId()
        {
            return 6;
        }

        public static int minPriceSinceVersion()
        {
            return 0;
        }

        public static int minPriceEncodingOffset()
        {
            return 36;
        }

        public static int minPriceEncodingLength()
        {
            return 8;
        }

        public static String minPriceMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long minPriceNullValue()
        {
            return -9223372036854775808L;
        }

        public static long minPriceMinValue()
        {
            return -9223372036854775807L;
        }

        public static long minPriceMaxValue()
        {
            return 9223372036854775807L;
        }

        public MarketsEncoder minPrice(final long value)
        {
            buffer.putLong(offset + 36, value, BYTE_ORDER);
            return this;
        }


        public static int maxPriceId()
        {
            return 7;
        }

        public static int maxPriceSinceVersion()
        {
            return 0;
        }

        public static int maxPriceEncodingOffset()
        {
            return 44;
        }

        public static int maxPriceEncodingLength()
        {
            return 8;
        }

        public static String maxPriceMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long maxPriceNullValue()
        {
            return -9223372036854775808L;
        }

        public static long maxPriceMinValue()
        {
            return -9223372036854775807L;
        }

        public static long maxPriceMaxValue()
        {
            return 9223372036854775807L;
        }

        public MarketsEncoder maxPrice(final long value)
        {
            buffer.putLong(offset + 44, value, BYTE_ORDER);
            return this;
        }


        public static int lotSizeId()
        {
            return 8;
        }

        public static int lotSizeSinceVersion()
        {
            return 0;
        }

        public static int lotSizeEncodingOffset()
        {
            return 52;
        }

        public static int lotSizeEncodingLength()
        {
            return 8;
        }

        public static String lotSizeMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long lotSizeNullValue()
        {
            return -9223372036854775808L;
        }

        public static long lotSizeMinValue()
        {
            return -9223372036854775807L;
        }

        public static long lotSizeMaxValue()
        {
            return 9223372036854775807L;
        }

        public MarketsEncoder lotSize(final long value)
        {
            buffer.putLong(offset + 52, value, BYTE_ORDER);
            return this;
        }

    }

    private final AssetsEncoder assets = new AssetsEncoder(this);

    public static long assetsId()
    {
        return 11;
    }

    public AssetsEncoder assetsCount(final int count)
    {
        assets.wrap(buffer, count);
        return assets;
    }

    public static final class AssetsEncoder
    {
        public static final int HEADER_SIZE = 4;
        private final CoreConfigEncoder parentMessage;
        private MutableDirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int initialLimit;

        AssetsEncoder(final CoreConfigEncoder parentMessage)
        {
            this.parentMessage = parentMessage;
        }

        public void wrap(final MutableDirectBuffer buffer, final int count)
        {
            if (count < 0 || count > 65534)
            {
                throw new IllegalArgumentException("count outside allowed range: count=" + count);
            }

            if (buffer != this.buffer)
            {
                this.buffer = buffer;
            }

            index = 0;
            this.count = count;
            final int limit = parentMessage.limit();
            initialLimit = limit;
            parentMessage.limit(limit + HEADER_SIZE);
            buffer.putShort(limit + 0, (short)13, BYTE_ORDER);
            buffer.putShort(limit + 2, (short)count, BYTE_ORDER);
        }

        public AssetsEncoder next()
        {
            if (index >= count)
            {
                throw new java.util.NoSuchElementException();
            }

            offset = parentMessage.limit();
            parentMessage.limit(offset + sbeBlockLength());
            ++index;

            return this;
        }

        public int resetCountToIndex()
        {
            count = index;
            buffer.putShort(initialLimit + 2, (short)count, BYTE_ORDER);

            return count;
        }

        public static int countMinValue()
        {
            return 0;
        }

        public static int countMaxValue()
        {
            return 65534;
        }

        public static int sbeHeaderSize()
        {
            return HEADER_SIZE;
        }

        public static int sbeBlockLength()
        {
            return 13;
        }

        public static int assetIdId()
        {
            return 1;
        }

        public static int assetIdSinceVersion()
        {
            return 0;
        }

        public static int assetIdEncodingOffset()
        {
            return 0;
        }

        public static int assetIdEncodingLength()
        {
            return 4;
        }

        public static String assetIdMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static int assetIdNullValue()
        {
            return -2147483648;
        }

        public static int assetIdMinValue()
        {
            return -2147483647;
        }

        public static int assetIdMaxValue()
        {
            return 2147483647;
        }

        public AssetsEncoder assetId(final int value)
        {
            buffer.putInt(offset + 0, value, BYTE_ORDER);
            return this;
        }


        public static int codeId()
        {
            return 2;
        }

        public static int codeSinceVersion()
        {
            return 0;
        }

        public static int codeEncodingOffset()
        {
            return 4;
        }

        public static int codeEncodingLength()
        {
            return 8;
        }

        public static String codeMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static byte codeNullValue()
        {
            return (byte)0;
        }

        public static byte codeMinValue()
        {
            return (byte)32;
        }

        public static byte codeMaxValue()
        {
            return (byte)126;
        }

        public static int codeLength()
        {
            return 8;
        }


        public AssetsEncoder code(final int index, final byte value)
        {
            if (index < 0 || index >= 8)
            {
                throw new IndexOutOfBoundsException("index out of range: index=" + index);
            }

            final int pos = offset + 4 + (index * 1);
            buffer.putByte(pos, value);

            return this;
        }

        public static String codeCharacterEncoding()
        {
            return java.nio.charset.StandardCharsets.US_ASCII.name();
        }

        public AssetsEncoder putCode(final byte[] src, final int srcOffset)
        {
            final int length = 8;
            if (srcOffset < 0 || srcOffset > (src.length - length))
            {
                throw new IndexOutOfBoundsException("Copy will go out of range: offset=" + srcOffset);
            }

            buffer.putBytes(offset + 4, src, srcOffset, length);

            return this;
        }

        public AssetsEncoder code(final String src)
        {
            final int length = 8;
            final int srcLength = null == src ? 0 : src.length();
            if (srcLength > length)
            {
                throw new IndexOutOfBoundsException("String too large for copy: byte length=" + srcLength);
            }

            buffer.putStringWithoutLengthAscii(offset + 4, src);

            for (int start = srcLength; start < length; ++start)
            {
                buffer.putByte(offset + 4 + start, (byte)0);
            }

            return this;
        }

        public AssetsEncoder code(final CharSequence src)
        {
            final int length = 8;
            final int srcLength = null == src ? 0 : src.length();
            if (srcLength > length)
            {
                throw new IndexOutOfBoundsException("CharSequence too large for copy: byte length=" + srcLength);
            }

            buffer.putStringWithoutLengthAscii(offset + 4, src);

            for (int start = srcLength; start < length; ++start)
            {
                buffer.putByte(offset + 4 + start, (byte)0);
            }

            return this;
        }

        public static int scaleId()
        {
            return 3;
        }

        public static int scaleSinceVersion()
        {
            return 0;
        }

        public static int scaleEncodingOffset()
        {
            return 12;
        }

        public static int scaleEncodingLength()
        {
            return 1;
        }

        public static String scaleMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static short scaleNullValue()
        {
            return (short)255;
        }

        public static short scaleMinValue()
        {
            return (short)0;
        }

        public static short scaleMaxValue()
        {
            return (short)254;
        }

        public AssetsEncoder scale(final short value)
        {
            buffer.putByte(offset + 12, (byte)value);
            return this;
        }

    }

    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        return appendTo(new StringBuilder()).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        if (null == buffer)
        {
            return builder;
        }

        final CoreConfigDecoder decoder = new CoreConfigDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
