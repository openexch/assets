/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * RC0 spike ingress: per-market risk limits (replicated; defaults apply until set)
 */
@SuppressWarnings("all")
public final class ProtoRiskConfigDecoder
{
    public static final int BLOCK_LENGTH = 61;
    public static final int TEMPLATE_ID = 44;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 5;
    public static final String SEMANTIC_VERSION = "0.5";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final ProtoRiskConfigDecoder parentMessage = this;
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

    public ProtoRiskConfigDecoder wrap(
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

    public ProtoRiskConfigDecoder wrapAndApplyHeader(
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

    public ProtoRiskConfigDecoder sbeRewind()
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


    public static int marketIdId()
    {
        return 2;
    }

    public static int marketIdSinceVersion()
    {
        return 0;
    }

    public static int marketIdEncodingOffset()
    {
        return 8;
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
        return buffer.getInt(offset + 8, BYTE_ORDER);
    }


    public static int maxOrdersPerSecId()
    {
        return 3;
    }

    public static int maxOrdersPerSecSinceVersion()
    {
        return 0;
    }

    public static int maxOrdersPerSecEncodingOffset()
    {
        return 12;
    }

    public static int maxOrdersPerSecEncodingLength()
    {
        return 4;
    }

    public static String maxOrdersPerSecMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int maxOrdersPerSecNullValue()
    {
        return -2147483648;
    }

    public static int maxOrdersPerSecMinValue()
    {
        return -2147483647;
    }

    public static int maxOrdersPerSecMaxValue()
    {
        return 2147483647;
    }

    public int maxOrdersPerSec()
    {
        return buffer.getInt(offset + 12, BYTE_ORDER);
    }


    public static int maxOrdersPerMinId()
    {
        return 4;
    }

    public static int maxOrdersPerMinSinceVersion()
    {
        return 0;
    }

    public static int maxOrdersPerMinEncodingOffset()
    {
        return 16;
    }

    public static int maxOrdersPerMinEncodingLength()
    {
        return 4;
    }

    public static String maxOrdersPerMinMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int maxOrdersPerMinNullValue()
    {
        return -2147483648;
    }

    public static int maxOrdersPerMinMinValue()
    {
        return -2147483647;
    }

    public static int maxOrdersPerMinMaxValue()
    {
        return 2147483647;
    }

    public int maxOrdersPerMin()
    {
        return buffer.getInt(offset + 16, BYTE_ORDER);
    }


    public static int minQtyId()
    {
        return 5;
    }

    public static int minQtySinceVersion()
    {
        return 0;
    }

    public static int minQtyEncodingOffset()
    {
        return 20;
    }

    public static int minQtyEncodingLength()
    {
        return 8;
    }

    public static String minQtyMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long minQtyNullValue()
    {
        return -9223372036854775808L;
    }

    public static long minQtyMinValue()
    {
        return -9223372036854775807L;
    }

    public static long minQtyMaxValue()
    {
        return 9223372036854775807L;
    }

    public long minQty()
    {
        return buffer.getLong(offset + 20, BYTE_ORDER);
    }


    public static int maxQtyId()
    {
        return 6;
    }

    public static int maxQtySinceVersion()
    {
        return 0;
    }

    public static int maxQtyEncodingOffset()
    {
        return 28;
    }

    public static int maxQtyEncodingLength()
    {
        return 8;
    }

    public static String maxQtyMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long maxQtyNullValue()
    {
        return -9223372036854775808L;
    }

    public static long maxQtyMinValue()
    {
        return -9223372036854775807L;
    }

    public static long maxQtyMaxValue()
    {
        return 9223372036854775807L;
    }

    public long maxQty()
    {
        return buffer.getLong(offset + 28, BYTE_ORDER);
    }


    public static int maxNotionalId()
    {
        return 7;
    }

    public static int maxNotionalSinceVersion()
    {
        return 0;
    }

    public static int maxNotionalEncodingOffset()
    {
        return 36;
    }

    public static int maxNotionalEncodingLength()
    {
        return 8;
    }

    public static String maxNotionalMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long maxNotionalNullValue()
    {
        return -9223372036854775808L;
    }

    public static long maxNotionalMinValue()
    {
        return -9223372036854775807L;
    }

    public static long maxNotionalMaxValue()
    {
        return 9223372036854775807L;
    }

    public long maxNotional()
    {
        return buffer.getLong(offset + 36, BYTE_ORDER);
    }


    public static int collarPctId()
    {
        return 8;
    }

    public static int collarPctSinceVersion()
    {
        return 0;
    }

    public static int collarPctEncodingOffset()
    {
        return 44;
    }

    public static int collarPctEncodingLength()
    {
        return 4;
    }

    public static String collarPctMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int collarPctNullValue()
    {
        return -2147483648;
    }

    public static int collarPctMinValue()
    {
        return -2147483647;
    }

    public static int collarPctMaxValue()
    {
        return 2147483647;
    }

    public int collarPct()
    {
        return buffer.getInt(offset + 44, BYTE_ORDER);
    }


    public static int maxOpenOrdersId()
    {
        return 9;
    }

    public static int maxOpenOrdersSinceVersion()
    {
        return 0;
    }

    public static int maxOpenOrdersEncodingOffset()
    {
        return 48;
    }

    public static int maxOpenOrdersEncodingLength()
    {
        return 4;
    }

    public static String maxOpenOrdersMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int maxOpenOrdersNullValue()
    {
        return -2147483648;
    }

    public static int maxOpenOrdersMinValue()
    {
        return -2147483647;
    }

    public static int maxOpenOrdersMaxValue()
    {
        return 2147483647;
    }

    public int maxOpenOrders()
    {
        return buffer.getInt(offset + 48, BYTE_ORDER);
    }


    public static int maxPositionPerMarketId()
    {
        return 10;
    }

    public static int maxPositionPerMarketSinceVersion()
    {
        return 0;
    }

    public static int maxPositionPerMarketEncodingOffset()
    {
        return 52;
    }

    public static int maxPositionPerMarketEncodingLength()
    {
        return 8;
    }

    public static String maxPositionPerMarketMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long maxPositionPerMarketNullValue()
    {
        return -9223372036854775808L;
    }

    public static long maxPositionPerMarketMinValue()
    {
        return -9223372036854775807L;
    }

    public static long maxPositionPerMarketMaxValue()
    {
        return 9223372036854775807L;
    }

    public long maxPositionPerMarket()
    {
        return buffer.getLong(offset + 52, BYTE_ORDER);
    }


    public static int circuitBreakerId()
    {
        return 11;
    }

    public static int circuitBreakerSinceVersion()
    {
        return 0;
    }

    public static int circuitBreakerEncodingOffset()
    {
        return 60;
    }

    public static int circuitBreakerEncodingLength()
    {
        return 1;
    }

    public static String circuitBreakerMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short circuitBreakerRaw()
    {
        return ((short)(buffer.getByte(offset + 60) & 0xFF));
    }

    public BoolFlag circuitBreaker()
    {
        return BoolFlag.get(((short)(buffer.getByte(offset + 60) & 0xFF)));
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final ProtoRiskConfigDecoder decoder = new ProtoRiskConfigDecoder();
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
        builder.append("[ProtoRiskConfig](sbeTemplateId=");
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
        builder.append("marketId=");
        builder.append(this.marketId());
        builder.append('|');
        builder.append("maxOrdersPerSec=");
        builder.append(this.maxOrdersPerSec());
        builder.append('|');
        builder.append("maxOrdersPerMin=");
        builder.append(this.maxOrdersPerMin());
        builder.append('|');
        builder.append("minQty=");
        builder.append(this.minQty());
        builder.append('|');
        builder.append("maxQty=");
        builder.append(this.maxQty());
        builder.append('|');
        builder.append("maxNotional=");
        builder.append(this.maxNotional());
        builder.append('|');
        builder.append("collarPct=");
        builder.append(this.collarPct());
        builder.append('|');
        builder.append("maxOpenOrders=");
        builder.append(this.maxOpenOrders());
        builder.append('|');
        builder.append("maxPositionPerMarket=");
        builder.append(this.maxPositionPerMarket());
        builder.append('|');
        builder.append("circuitBreaker=");
        builder.append(this.circuitBreaker());

        limit(originalLimit);

        return builder;
    }
    
    public ProtoRiskConfigDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
