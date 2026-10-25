/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * Full risk record per market (marketId 0 = INVALID; critic C10), monotonic configVersion, basis points everywhere (exact tryMultiply(x, bps) / 10000). Replicated and snapshotted. Rate state is fixed cluster-time buckets, so raising a limit resizes nothing.
 */
@SuppressWarnings("all")
public final class RiskConfigSetEncoder
{
    public static final int BLOCK_LENGTH = 76;
    public static final int TEMPLATE_ID = 71;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final RiskConfigSetEncoder parentMessage = this;
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

    public RiskConfigSetEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public RiskConfigSetEncoder wrapAndApplyHeader(
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

    public RiskConfigSetEncoder correlationId(final long value)
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

    public RiskConfigSetEncoder configVersion(final long value)
    {
        buffer.putInt(offset + 8, (int)value, BYTE_ORDER);
        return this;
    }


    public static int marketIdId()
    {
        return 3;
    }

    public static int marketIdSinceVersion()
    {
        return 0;
    }

    public static int marketIdEncodingOffset()
    {
        return 12;
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

    public RiskConfigSetEncoder marketId(final int value)
    {
        buffer.putInt(offset + 12, value, BYTE_ORDER);
        return this;
    }


    public static int minQuantityId()
    {
        return 4;
    }

    public static int minQuantitySinceVersion()
    {
        return 0;
    }

    public static int minQuantityEncodingOffset()
    {
        return 16;
    }

    public static int minQuantityEncodingLength()
    {
        return 8;
    }

    public static String minQuantityMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long minQuantityNullValue()
    {
        return -9223372036854775808L;
    }

    public static long minQuantityMinValue()
    {
        return -9223372036854775807L;
    }

    public static long minQuantityMaxValue()
    {
        return 9223372036854775807L;
    }

    public RiskConfigSetEncoder minQuantity(final long value)
    {
        buffer.putLong(offset + 16, value, BYTE_ORDER);
        return this;
    }


    public static int maxQuantityId()
    {
        return 5;
    }

    public static int maxQuantitySinceVersion()
    {
        return 0;
    }

    public static int maxQuantityEncodingOffset()
    {
        return 24;
    }

    public static int maxQuantityEncodingLength()
    {
        return 8;
    }

    public static String maxQuantityMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long maxQuantityNullValue()
    {
        return -9223372036854775808L;
    }

    public static long maxQuantityMinValue()
    {
        return -9223372036854775807L;
    }

    public static long maxQuantityMaxValue()
    {
        return 9223372036854775807L;
    }

    public RiskConfigSetEncoder maxQuantity(final long value)
    {
        buffer.putLong(offset + 24, value, BYTE_ORDER);
        return this;
    }


    public static int minNotionalId()
    {
        return 6;
    }

    public static int minNotionalSinceVersion()
    {
        return 0;
    }

    public static int minNotionalEncodingOffset()
    {
        return 32;
    }

    public static int minNotionalEncodingLength()
    {
        return 8;
    }

    public static String minNotionalMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long minNotionalNullValue()
    {
        return -9223372036854775808L;
    }

    public static long minNotionalMinValue()
    {
        return -9223372036854775807L;
    }

    public static long minNotionalMaxValue()
    {
        return 9223372036854775807L;
    }

    public RiskConfigSetEncoder minNotional(final long value)
    {
        buffer.putLong(offset + 32, value, BYTE_ORDER);
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
        return 40;
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

    public RiskConfigSetEncoder maxNotional(final long value)
    {
        buffer.putLong(offset + 40, value, BYTE_ORDER);
        return this;
    }


    public static int priceCollarBpsId()
    {
        return 8;
    }

    public static int priceCollarBpsSinceVersion()
    {
        return 0;
    }

    public static int priceCollarBpsEncodingOffset()
    {
        return 48;
    }

    public static int priceCollarBpsEncodingLength()
    {
        return 4;
    }

    public static String priceCollarBpsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long priceCollarBpsNullValue()
    {
        return 4294967295L;
    }

    public static long priceCollarBpsMinValue()
    {
        return 0L;
    }

    public static long priceCollarBpsMaxValue()
    {
        return 4294967294L;
    }

    public RiskConfigSetEncoder priceCollarBps(final long value)
    {
        buffer.putInt(offset + 48, (int)value, BYTE_ORDER);
        return this;
    }


    public static int maxOrdersPerSecId()
    {
        return 9;
    }

    public static int maxOrdersPerSecSinceVersion()
    {
        return 0;
    }

    public static int maxOrdersPerSecEncodingOffset()
    {
        return 52;
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

    public RiskConfigSetEncoder maxOrdersPerSec(final int value)
    {
        buffer.putInt(offset + 52, value, BYTE_ORDER);
        return this;
    }


    public static int maxOrdersPerMinId()
    {
        return 10;
    }

    public static int maxOrdersPerMinSinceVersion()
    {
        return 0;
    }

    public static int maxOrdersPerMinEncodingOffset()
    {
        return 56;
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

    public RiskConfigSetEncoder maxOrdersPerMin(final int value)
    {
        buffer.putInt(offset + 56, value, BYTE_ORDER);
        return this;
    }


    public static int maxOpenOrdersId()
    {
        return 11;
    }

    public static int maxOpenOrdersSinceVersion()
    {
        return 0;
    }

    public static int maxOpenOrdersEncodingOffset()
    {
        return 60;
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

    public RiskConfigSetEncoder maxOpenOrders(final int value)
    {
        buffer.putInt(offset + 60, value, BYTE_ORDER);
        return this;
    }


    public static int maxPositionPerMarketId()
    {
        return 12;
    }

    public static int maxPositionPerMarketSinceVersion()
    {
        return 0;
    }

    public static int maxPositionPerMarketEncodingOffset()
    {
        return 64;
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

    public RiskConfigSetEncoder maxPositionPerMarket(final long value)
    {
        buffer.putLong(offset + 64, value, BYTE_ORDER);
        return this;
    }


    public static int marketBuySlippageBpsId()
    {
        return 13;
    }

    public static int marketBuySlippageBpsSinceVersion()
    {
        return 0;
    }

    public static int marketBuySlippageBpsEncodingOffset()
    {
        return 72;
    }

    public static int marketBuySlippageBpsEncodingLength()
    {
        return 4;
    }

    public static String marketBuySlippageBpsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long marketBuySlippageBpsNullValue()
    {
        return 4294967295L;
    }

    public static long marketBuySlippageBpsMinValue()
    {
        return 0L;
    }

    public static long marketBuySlippageBpsMaxValue()
    {
        return 4294967294L;
    }

    public RiskConfigSetEncoder marketBuySlippageBps(final long value)
    {
        buffer.putInt(offset + 72, (int)value, BYTE_ORDER);
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

        final RiskConfigSetDecoder decoder = new RiskConfigSetDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
