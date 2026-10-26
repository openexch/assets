/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * Forwarder-stitched top of book (K4; critic D14: 6 fields + meEpoch, quotes only). The core ignores a tick whose bookVersion is not above the stored one, so N forwarders are safe. State only, no egress.
 */
@SuppressWarnings("all")
public final class MarketTickDecoder
{
    public static final int BLOCK_LENGTH = 52;
    public static final int TEMPLATE_ID = 60;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final MarketTickDecoder parentMessage = this;
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

    public MarketTickDecoder wrap(
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

    public MarketTickDecoder wrapAndApplyHeader(
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

    public MarketTickDecoder sbeRewind()
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

    public long bookVersion()
    {
        return buffer.getLong(offset + 4, BYTE_ORDER);
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

    public long bestBid()
    {
        return buffer.getLong(offset + 12, BYTE_ORDER);
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

    public long bestAsk()
    {
        return buffer.getLong(offset + 20, BYTE_ORDER);
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

    public long lastTradePrice()
    {
        return buffer.getLong(offset + 28, BYTE_ORDER);
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

    public long lastTradeId()
    {
        return buffer.getLong(offset + 36, BYTE_ORDER);
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

    public long meEpoch()
    {
        return buffer.getLong(offset + 44, BYTE_ORDER);
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final MarketTickDecoder decoder = new MarketTickDecoder();
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
        builder.append("[MarketTick](sbeTemplateId=");
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
        builder.append("marketId=");
        builder.append(this.marketId());
        builder.append('|');
        builder.append("bookVersion=");
        builder.append(this.bookVersion());
        builder.append('|');
        builder.append("bestBid=");
        builder.append(this.bestBid());
        builder.append('|');
        builder.append("bestAsk=");
        builder.append(this.bestAsk());
        builder.append('|');
        builder.append("lastTradePrice=");
        builder.append(this.lastTradePrice());
        builder.append('|');
        builder.append("lastTradeId=");
        builder.append(this.lastTradeId());
        builder.append('|');
        builder.append("meEpoch=");
        builder.append(this.meEpoch());

        limit(originalLimit);

        return builder;
    }
    
    public MarketTickDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
