/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * Forwarder-stitched top of book (K4; critic D14: 6 fields + meEpoch, quotes only). The core ignores a tick whose bookVersion is not above the stored one, so N forwarders are safe. State only, no egress.
 */
@SuppressWarnings("all")
public final class MarketTickEncoder
{
    public static final int BLOCK_LENGTH = 52;
    public static final int TEMPLATE_ID = 60;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final MarketTickEncoder parentMessage = this;
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

    public MarketTickEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public MarketTickEncoder wrapAndApplyHeader(
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

    public MarketTickEncoder marketId(final int value)
    {
        buffer.putInt(offset + 0, value, BYTE_ORDER);
        return this;
    }


    public static int bookVersionId()
    {
        return 2;
    }

    public static int bookVersionSinceVersion()
    {
        return 0;
    }

    public static int bookVersionEncodingOffset()
    {
        return 4;
    }

    public static int bookVersionEncodingLength()
    {
        return 8;
    }

    public static String bookVersionMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long bookVersionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long bookVersionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long bookVersionMaxValue()
    {
        return 9223372036854775807L;
    }

    public MarketTickEncoder bookVersion(final long value)
    {
        buffer.putLong(offset + 4, value, BYTE_ORDER);
        return this;
    }


    public static int bestBidId()
    {
        return 3;
    }

    public static int bestBidSinceVersion()
    {
        return 0;
    }

    public static int bestBidEncodingOffset()
    {
        return 12;
    }

    public static int bestBidEncodingLength()
    {
        return 8;
    }

    public static String bestBidMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long bestBidNullValue()
    {
        return -9223372036854775808L;
    }

    public static long bestBidMinValue()
    {
        return -9223372036854775807L;
    }

    public static long bestBidMaxValue()
    {
        return 9223372036854775807L;
    }

    public MarketTickEncoder bestBid(final long value)
    {
        buffer.putLong(offset + 12, value, BYTE_ORDER);
        return this;
    }


    public static int bestAskId()
    {
        return 4;
    }

    public static int bestAskSinceVersion()
    {
        return 0;
    }

    public static int bestAskEncodingOffset()
    {
        return 20;
    }

    public static int bestAskEncodingLength()
    {
        return 8;
    }

    public static String bestAskMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long bestAskNullValue()
    {
        return -9223372036854775808L;
    }

    public static long bestAskMinValue()
    {
        return -9223372036854775807L;
    }

    public static long bestAskMaxValue()
    {
        return 9223372036854775807L;
    }

    public MarketTickEncoder bestAsk(final long value)
    {
        buffer.putLong(offset + 20, value, BYTE_ORDER);
        return this;
    }


    public static int lastTradePriceId()
    {
        return 5;
    }

    public static int lastTradePriceSinceVersion()
    {
        return 0;
    }

    public static int lastTradePriceEncodingOffset()
    {
        return 28;
    }

    public static int lastTradePriceEncodingLength()
    {
        return 8;
    }

    public static String lastTradePriceMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long lastTradePriceNullValue()
    {
        return -9223372036854775808L;
    }

    public static long lastTradePriceMinValue()
    {
        return -9223372036854775807L;
    }

    public static long lastTradePriceMaxValue()
    {
        return 9223372036854775807L;
    }

    public MarketTickEncoder lastTradePrice(final long value)
    {
        buffer.putLong(offset + 28, value, BYTE_ORDER);
        return this;
    }


    public static int lastTradeIdId()
    {
        return 6;
    }

    public static int lastTradeIdSinceVersion()
    {
        return 0;
    }

    public static int lastTradeIdEncodingOffset()
    {
        return 36;
    }

    public static int lastTradeIdEncodingLength()
    {
        return 8;
    }

    public static String lastTradeIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long lastTradeIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long lastTradeIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long lastTradeIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public MarketTickEncoder lastTradeId(final long value)
    {
        buffer.putLong(offset + 36, value, BYTE_ORDER);
        return this;
    }


    public static int meEpochId()
    {
        return 7;
    }

    public static int meEpochSinceVersion()
    {
        return 0;
    }

    public static int meEpochEncodingOffset()
    {
        return 44;
    }

    public static int meEpochEncodingLength()
    {
        return 8;
    }

    public static String meEpochMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long meEpochNullValue()
    {
        return -9223372036854775808L;
    }

    public static long meEpochMinValue()
    {
        return -9223372036854775807L;
    }

    public static long meEpochMaxValue()
    {
        return 9223372036854775807L;
    }

    public MarketTickEncoder meEpoch(final long value)
    {
        buffer.putLong(offset + 44, value, BYTE_ORDER);
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

        final MarketTickDecoder decoder = new MarketTickDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
