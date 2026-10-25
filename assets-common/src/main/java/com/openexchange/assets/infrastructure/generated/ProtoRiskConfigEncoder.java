/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * RC0 spike ingress: per-market risk limits (replicated; defaults apply until set)
 */
@SuppressWarnings("all")
public final class ProtoRiskConfigEncoder
{
    public static final int BLOCK_LENGTH = 61;
    public static final int TEMPLATE_ID = 44;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 5;
    public static final String SEMANTIC_VERSION = "0.5";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final ProtoRiskConfigEncoder parentMessage = this;
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

    public ProtoRiskConfigEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public ProtoRiskConfigEncoder wrapAndApplyHeader(
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

    public ProtoRiskConfigEncoder correlationId(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
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

    public ProtoRiskConfigEncoder marketId(final int value)
    {
        buffer.putInt(offset + 8, value, BYTE_ORDER);
        return this;
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

    public ProtoRiskConfigEncoder maxOrdersPerSec(final int value)
    {
        buffer.putInt(offset + 12, value, BYTE_ORDER);
        return this;
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

    public ProtoRiskConfigEncoder maxOrdersPerMin(final int value)
    {
        buffer.putInt(offset + 16, value, BYTE_ORDER);
        return this;
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

    public ProtoRiskConfigEncoder minQty(final long value)
    {
        buffer.putLong(offset + 20, value, BYTE_ORDER);
        return this;
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

    public ProtoRiskConfigEncoder maxQty(final long value)
    {
        buffer.putLong(offset + 28, value, BYTE_ORDER);
        return this;
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

    public ProtoRiskConfigEncoder maxNotional(final long value)
    {
        buffer.putLong(offset + 36, value, BYTE_ORDER);
        return this;
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

    public ProtoRiskConfigEncoder collarPct(final int value)
    {
        buffer.putInt(offset + 44, value, BYTE_ORDER);
        return this;
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

    public ProtoRiskConfigEncoder maxOpenOrders(final int value)
    {
        buffer.putInt(offset + 48, value, BYTE_ORDER);
        return this;
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

    public ProtoRiskConfigEncoder maxPositionPerMarket(final long value)
    {
        buffer.putLong(offset + 52, value, BYTE_ORDER);
        return this;
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

    public ProtoRiskConfigEncoder circuitBreaker(final BoolFlag value)
    {
        buffer.putByte(offset + 60, (byte)value.value());
        return this;
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

        final ProtoRiskConfigDecoder decoder = new ProtoRiskConfigDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
