/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * Full risk record per market (marketId 0 = INVALID; critic C10), monotonic configVersion, basis points everywhere (exact tryMultiply(x, bps) / 10000). Replicated and snapshotted. Rate state is fixed cluster-time buckets, so raising a limit resizes nothing.
 */
@SuppressWarnings("all")
public final class RiskConfigSetDecoder
{
    public static final int BLOCK_LENGTH = 76;
    public static final int TEMPLATE_ID = 71;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final RiskConfigSetDecoder parentMessage = this;
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

    public RiskConfigSetDecoder wrap(
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

    public RiskConfigSetDecoder wrapAndApplyHeader(
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

    public RiskConfigSetDecoder sbeRewind()
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

    public int marketId()
    {
        return buffer.getInt(offset + 12, BYTE_ORDER);
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

    public long minQuantity()
    {
        return buffer.getLong(offset + 16, BYTE_ORDER);
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

    public long maxQuantity()
    {
        return buffer.getLong(offset + 24, BYTE_ORDER);
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

    public long minNotional()
    {
        return buffer.getLong(offset + 32, BYTE_ORDER);
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

    public long maxNotional()
    {
        return buffer.getLong(offset + 40, BYTE_ORDER);
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

    public long priceCollarBps()
    {
        return (buffer.getInt(offset + 48, BYTE_ORDER) & 0xFFFF_FFFFL);
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

    public int maxOrdersPerSec()
    {
        return buffer.getInt(offset + 52, BYTE_ORDER);
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

    public int maxOrdersPerMin()
    {
        return buffer.getInt(offset + 56, BYTE_ORDER);
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

    public int maxOpenOrders()
    {
        return buffer.getInt(offset + 60, BYTE_ORDER);
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

    public long maxPositionPerMarket()
    {
        return buffer.getLong(offset + 64, BYTE_ORDER);
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

    public long marketBuySlippageBps()
    {
        return (buffer.getInt(offset + 72, BYTE_ORDER) & 0xFFFF_FFFFL);
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final RiskConfigSetDecoder decoder = new RiskConfigSetDecoder();
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
        builder.append("[RiskConfigSet](sbeTemplateId=");
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
        builder.append("marketId=");
        builder.append(this.marketId());
        builder.append('|');
        builder.append("minQuantity=");
        builder.append(this.minQuantity());
        builder.append('|');
        builder.append("maxQuantity=");
        builder.append(this.maxQuantity());
        builder.append('|');
        builder.append("minNotional=");
        builder.append(this.minNotional());
        builder.append('|');
        builder.append("maxNotional=");
        builder.append(this.maxNotional());
        builder.append('|');
        builder.append("priceCollarBps=");
        builder.append(this.priceCollarBps());
        builder.append('|');
        builder.append("maxOrdersPerSec=");
        builder.append(this.maxOrdersPerSec());
        builder.append('|');
        builder.append("maxOrdersPerMin=");
        builder.append(this.maxOrdersPerMin());
        builder.append('|');
        builder.append("maxOpenOrders=");
        builder.append(this.maxOpenOrders());
        builder.append('|');
        builder.append("maxPositionPerMarket=");
        builder.append(this.maxPositionPerMarket());
        builder.append('|');
        builder.append("marketBuySlippageBps=");
        builder.append(this.marketBuySlippageBps());

        limit(originalLimit);

        return builder;
    }
    
    public RiskConfigSetDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
