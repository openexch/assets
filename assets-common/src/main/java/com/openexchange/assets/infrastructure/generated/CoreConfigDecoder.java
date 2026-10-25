/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * Markets, assets and the behaviour version as a logged command (EngineConfig pattern). Decision table: recorded+equal = DUPLICATE, recorded+different = REJECTED, fresh = record, adopt = cross-check compiled tables and fail fast on mismatch. The assets group may change only while no account exists.
 */
@SuppressWarnings("all")
public final class CoreConfigDecoder
{
    public static final int BLOCK_LENGTH = 16;
    public static final int TEMPLATE_ID = 70;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final CoreConfigDecoder parentMessage = this;
    private DirectBuffer buffer;
    private int offset;
    private int limit;
    int actingBlockLength;
    int actingVersion;

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

    public DirectBuffer buffer()
    {
        return buffer;
    }

    public int offset()
    {
        return offset;
    }

    public CoreConfigDecoder wrap(
        final DirectBuffer buffer,
        final int offset,
        final int actingBlockLength,
        final int actingVersion)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        this.actingBlockLength = actingBlockLength;
        this.actingVersion = actingVersion;
        limit(offset + actingBlockLength);

        return this;
    }

    public CoreConfigDecoder wrapAndApplyHeader(
        final DirectBuffer buffer,
        final int offset,
        final MessageHeaderDecoder headerDecoder)
    {
        headerDecoder.wrap(buffer, offset);

        final int templateId = headerDecoder.templateId();
        if (TEMPLATE_ID != templateId)
        {
            throw new IllegalStateException("Invalid TEMPLATE_ID: " + templateId);
        }

        return wrap(
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            headerDecoder.blockLength(),
            headerDecoder.version());
    }

    public CoreConfigDecoder sbeRewind()
    {
        return wrap(buffer, offset, actingBlockLength, actingVersion);
    }

    public int sbeDecodedLength()
    {
        final int currentLimit = limit();
        sbeSkip();
        final int decodedLength = encodedLength();
        limit(currentLimit);

        return decodedLength;
    }

    public int actingVersion()
    {
        return actingVersion;
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

    public long correlationId()
    {
        return buffer.getLong(offset + 0, BYTE_ORDER);
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

    public long configVersion()
    {
        return (buffer.getInt(offset + 8, BYTE_ORDER) & 0xFFFF_FFFFL);
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

    public long behaviorVersion()
    {
        return (buffer.getInt(offset + 12, BYTE_ORDER) & 0xFFFF_FFFFL);
    }


    private final MarketsDecoder markets = new MarketsDecoder(this);

    public static long marketsDecoderId()
    {
        return 10;
    }

    public static int marketsDecoderSinceVersion()
    {
        return 0;
    }

    public MarketsDecoder markets()
    {
        markets.wrap(buffer);
        return markets;
    }

    public static final class MarketsDecoder
        implements Iterable<MarketsDecoder>, java.util.Iterator<MarketsDecoder>
    {
        public static final int HEADER_SIZE = 4;
        private final CoreConfigDecoder parentMessage;
        private DirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int blockLength;

        MarketsDecoder(final CoreConfigDecoder parentMessage)
        {
            this.parentMessage = parentMessage;
        }

        public void wrap(final DirectBuffer buffer)
        {
            if (buffer != this.buffer)
            {
                this.buffer = buffer;
            }

            index = 0;
            final int limit = parentMessage.limit();
            parentMessage.limit(limit + HEADER_SIZE);
            blockLength = (buffer.getShort(limit + 0, BYTE_ORDER) & 0xFFFF);
            count = (buffer.getShort(limit + 2, BYTE_ORDER) & 0xFFFF);
        }

        public MarketsDecoder next()
        {
            if (index >= count)
            {
                throw new java.util.NoSuchElementException();
            }

            offset = parentMessage.limit();
            parentMessage.limit(offset + blockLength);
            ++index;

            return this;
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

        public int actingBlockLength()
        {
            return blockLength;
        }

        public int actingVersion()
        {
            return parentMessage.actingVersion;
        }

        public int count()
        {
            return count;
        }

        public java.util.Iterator<MarketsDecoder> iterator()
        {
            return this;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public boolean hasNext()
        {
            return index < count;
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

        public int marketId()
        {
            return buffer.getInt(offset + 0, BYTE_ORDER);
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


        public byte symbol(final int index)
        {
            if (index < 0 || index >= 16)
            {
                throw new IndexOutOfBoundsException("index out of range: index=" + index);
            }

            final int pos = offset + 4 + (index * 1);

            return buffer.getByte(pos);
        }


        public static String symbolCharacterEncoding()
        {
            return java.nio.charset.StandardCharsets.US_ASCII.name();
        }

        public int getSymbol(final byte[] dst, final int dstOffset)
        {
            final int length = 16;
            if (dstOffset < 0 || dstOffset > (dst.length - length))
            {
                throw new IndexOutOfBoundsException("Copy will go out of range: offset=" + dstOffset);
            }

            buffer.getBytes(offset + 4, dst, dstOffset, length);

            return length;
        }

        public String symbol()
        {
            final byte[] dst = new byte[16];
            buffer.getBytes(offset + 4, dst, 0, 16);

            int end = 0;
            for (; end < 16 && dst[end] != 0; ++end);

            return new String(dst, 0, end, java.nio.charset.StandardCharsets.US_ASCII);
        }


        public int getSymbol(final Appendable value)
        {
            for (int i = 0; i < 16; ++i)
            {
                final int c = buffer.getByte(offset + 4 + i) & 0xFF;
                if (c == 0)
                {
                    return i;
                }

                try
                {
                    value.append(c > 127 ? '?' : (char)c);
                }
                catch (final java.io.IOException ex)
                {
                    throw new java.io.UncheckedIOException(ex);
                }
            }

            return 16;
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

        public int baseAssetId()
        {
            return buffer.getInt(offset + 20, BYTE_ORDER);
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

        public int quoteAssetId()
        {
            return buffer.getInt(offset + 24, BYTE_ORDER);
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

        public long tickSize()
        {
            return buffer.getLong(offset + 28, BYTE_ORDER);
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

        public long minPrice()
        {
            return buffer.getLong(offset + 36, BYTE_ORDER);
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

        public long maxPrice()
        {
            return buffer.getLong(offset + 44, BYTE_ORDER);
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

        public long lotSize()
        {
            return buffer.getLong(offset + 52, BYTE_ORDER);
        }


        public StringBuilder appendTo(final StringBuilder builder)
        {
            if (null == buffer)
            {
                return builder;
            }

            builder.append('(');
            builder.append("marketId=");
            builder.append(this.marketId());
            builder.append('|');
            builder.append("symbol=");
            for (int i = 0; i < symbolLength() && this.symbol(i) > 0; i++)
            {
                builder.append((char)this.symbol(i));
            }
            builder.append('|');
            builder.append("baseAssetId=");
            builder.append(this.baseAssetId());
            builder.append('|');
            builder.append("quoteAssetId=");
            builder.append(this.quoteAssetId());
            builder.append('|');
            builder.append("tickSize=");
            builder.append(this.tickSize());
            builder.append('|');
            builder.append("minPrice=");
            builder.append(this.minPrice());
            builder.append('|');
            builder.append("maxPrice=");
            builder.append(this.maxPrice());
            builder.append('|');
            builder.append("lotSize=");
            builder.append(this.lotSize());
            builder.append(')');

            return builder;
        }
        
        public MarketsDecoder sbeSkip()
        {

            return this;
        }
    }

    private final AssetsDecoder assets = new AssetsDecoder(this);

    public static long assetsDecoderId()
    {
        return 11;
    }

    public static int assetsDecoderSinceVersion()
    {
        return 0;
    }

    public AssetsDecoder assets()
    {
        assets.wrap(buffer);
        return assets;
    }

    public static final class AssetsDecoder
        implements Iterable<AssetsDecoder>, java.util.Iterator<AssetsDecoder>
    {
        public static final int HEADER_SIZE = 4;
        private final CoreConfigDecoder parentMessage;
        private DirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int blockLength;

        AssetsDecoder(final CoreConfigDecoder parentMessage)
        {
            this.parentMessage = parentMessage;
        }

        public void wrap(final DirectBuffer buffer)
        {
            if (buffer != this.buffer)
            {
                this.buffer = buffer;
            }

            index = 0;
            final int limit = parentMessage.limit();
            parentMessage.limit(limit + HEADER_SIZE);
            blockLength = (buffer.getShort(limit + 0, BYTE_ORDER) & 0xFFFF);
            count = (buffer.getShort(limit + 2, BYTE_ORDER) & 0xFFFF);
        }

        public AssetsDecoder next()
        {
            if (index >= count)
            {
                throw new java.util.NoSuchElementException();
            }

            offset = parentMessage.limit();
            parentMessage.limit(offset + blockLength);
            ++index;

            return this;
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

        public int actingBlockLength()
        {
            return blockLength;
        }

        public int actingVersion()
        {
            return parentMessage.actingVersion;
        }

        public int count()
        {
            return count;
        }

        public java.util.Iterator<AssetsDecoder> iterator()
        {
            return this;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public boolean hasNext()
        {
            return index < count;
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

        public int assetId()
        {
            return buffer.getInt(offset + 0, BYTE_ORDER);
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


        public byte code(final int index)
        {
            if (index < 0 || index >= 8)
            {
                throw new IndexOutOfBoundsException("index out of range: index=" + index);
            }

            final int pos = offset + 4 + (index * 1);

            return buffer.getByte(pos);
        }


        public static String codeCharacterEncoding()
        {
            return java.nio.charset.StandardCharsets.US_ASCII.name();
        }

        public int getCode(final byte[] dst, final int dstOffset)
        {
            final int length = 8;
            if (dstOffset < 0 || dstOffset > (dst.length - length))
            {
                throw new IndexOutOfBoundsException("Copy will go out of range: offset=" + dstOffset);
            }

            buffer.getBytes(offset + 4, dst, dstOffset, length);

            return length;
        }

        public String code()
        {
            final byte[] dst = new byte[8];
            buffer.getBytes(offset + 4, dst, 0, 8);

            int end = 0;
            for (; end < 8 && dst[end] != 0; ++end);

            return new String(dst, 0, end, java.nio.charset.StandardCharsets.US_ASCII);
        }


        public int getCode(final Appendable value)
        {
            for (int i = 0; i < 8; ++i)
            {
                final int c = buffer.getByte(offset + 4 + i) & 0xFF;
                if (c == 0)
                {
                    return i;
                }

                try
                {
                    value.append(c > 127 ? '?' : (char)c);
                }
                catch (final java.io.IOException ex)
                {
                    throw new java.io.UncheckedIOException(ex);
                }
            }

            return 8;
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

        public short scale()
        {
            return ((short)(buffer.getByte(offset + 12) & 0xFF));
        }


        public StringBuilder appendTo(final StringBuilder builder)
        {
            if (null == buffer)
            {
                return builder;
            }

            builder.append('(');
            builder.append("assetId=");
            builder.append(this.assetId());
            builder.append('|');
            builder.append("code=");
            for (int i = 0; i < codeLength() && this.code(i) > 0; i++)
            {
                builder.append((char)this.code(i));
            }
            builder.append('|');
            builder.append("scale=");
            builder.append(this.scale());
            builder.append(')');

            return builder;
        }
        
        public AssetsDecoder sbeSkip()
        {

            return this;
        }
    }

    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final CoreConfigDecoder decoder = new CoreConfigDecoder();
        decoder.wrap(buffer, offset, actingBlockLength, actingVersion);

        return decoder.appendTo(new StringBuilder()).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        if (null == buffer)
        {
            return builder;
        }

        final int originalLimit = limit();
        limit(offset + actingBlockLength);
        builder.append("[CoreConfig](sbeTemplateId=");
        builder.append(TEMPLATE_ID);
        builder.append("|sbeSchemaId=");
        builder.append(SCHEMA_ID);
        builder.append("|sbeSchemaVersion=");
        if (parentMessage.actingVersion != SCHEMA_VERSION)
        {
            builder.append(parentMessage.actingVersion);
            builder.append('/');
        }
        builder.append(SCHEMA_VERSION);
        builder.append("|sbeBlockLength=");
        if (actingBlockLength != BLOCK_LENGTH)
        {
            builder.append(actingBlockLength);
            builder.append('/');
        }
        builder.append(BLOCK_LENGTH);
        builder.append("):");
        builder.append("correlationId=");
        builder.append(this.correlationId());
        builder.append('|');
        builder.append("configVersion=");
        builder.append(this.configVersion());
        builder.append('|');
        builder.append("behaviorVersion=");
        builder.append(this.behaviorVersion());
        builder.append('|');
        builder.append("markets=[");
        final int marketsOriginalOffset = markets.offset;
        final int marketsOriginalIndex = markets.index;
        final MarketsDecoder markets = this.markets();
        if (markets.count() > 0)
        {
            while (markets.hasNext())
            {
                markets.next().appendTo(builder);
                builder.append(',');
            }
            builder.setLength(builder.length() - 1);
        }
        markets.offset = marketsOriginalOffset;
        markets.index = marketsOriginalIndex;
        builder.append(']');
        builder.append('|');
        builder.append("assets=[");
        final int assetsOriginalOffset = assets.offset;
        final int assetsOriginalIndex = assets.index;
        final AssetsDecoder assets = this.assets();
        if (assets.count() > 0)
        {
            while (assets.hasNext())
            {
                assets.next().appendTo(builder);
                builder.append(',');
            }
            builder.setLength(builder.length() - 1);
        }
        assets.offset = assetsOriginalOffset;
        assets.index = assetsOriginalIndex;
        builder.append(']');

        limit(originalLimit);

        return builder;
    }
    
    public CoreConfigDecoder sbeSkip()
    {
        sbeRewind();
        MarketsDecoder markets = this.markets();
        if (markets.count() > 0)
        {
            while (markets.hasNext())
            {
                markets.next();
                markets.sbeSkip();
            }
        }
        AssetsDecoder assets = this.assets();
        if (assets.count() > 0)
        {
            while (assets.hasNext())
            {
                assets.next();
                assets.sbeSkip();
            }
        }

        return this;
    }
}
