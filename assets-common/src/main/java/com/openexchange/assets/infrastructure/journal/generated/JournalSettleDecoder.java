/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.journal.generated;

import org.agrona.DirectBuffer;


/**
 * An APPLIED settle (deduped re-deliveries journal nothing): one record per trade; the projector explodes it into per-leg rows. The four *After fields are post-settle (available + locked) totals and are AUTHORITATIVE: on a faulted leg (SettleFault on the money egress) the moved amount can be less than price*quantity, and the totals reflect what actually moved. clusterTimeMs = deterministic cluster timestamp of the applying log entry.
 */
@SuppressWarnings("all")
public final class JournalSettleDecoder
{
    public static final int BLOCK_LENGTH = 93;
    public static final int TEMPLATE_ID = 4;
    public static final int SCHEMA_ID = 3;
    public static final int SCHEMA_VERSION = 1;
    public static final String SEMANTIC_VERSION = "0.1";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final JournalSettleDecoder parentMessage = this;
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

    public JournalSettleDecoder wrap(
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

    public JournalSettleDecoder wrapAndApplyHeader(
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

    public JournalSettleDecoder sbeRewind()
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

    public static int journalSeqId()
    {
        return 1;
    }

    public static int journalSeqSinceVersion()
    {
        return 0;
    }

    public static int journalSeqEncodingOffset()
    {
        return 0;
    }

    public static int journalSeqEncodingLength()
    {
        return 8;
    }

    public static String journalSeqMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long journalSeqNullValue()
    {
        return -9223372036854775808L;
    }

    public static long journalSeqMinValue()
    {
        return -9223372036854775807L;
    }

    public static long journalSeqMaxValue()
    {
        return 9223372036854775807L;
    }

    public long journalSeq()
    {
        return buffer.getLong(offset + 0, BYTE_ORDER);
    }


    public static int tradeIdId()
    {
        return 2;
    }

    public static int tradeIdSinceVersion()
    {
        return 0;
    }

    public static int tradeIdEncodingOffset()
    {
        return 8;
    }

    public static int tradeIdEncodingLength()
    {
        return 8;
    }

    public static String tradeIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long tradeIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long tradeIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long tradeIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long tradeId()
    {
        return buffer.getLong(offset + 8, BYTE_ORDER);
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
        return 16;
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
        return buffer.getInt(offset + 16, BYTE_ORDER);
    }


    public static int priceId()
    {
        return 4;
    }

    public static int priceSinceVersion()
    {
        return 0;
    }

    public static int priceEncodingOffset()
    {
        return 20;
    }

    public static int priceEncodingLength()
    {
        return 8;
    }

    public static String priceMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long priceNullValue()
    {
        return -9223372036854775808L;
    }

    public static long priceMinValue()
    {
        return -9223372036854775807L;
    }

    public static long priceMaxValue()
    {
        return 9223372036854775807L;
    }

    public long price()
    {
        return buffer.getLong(offset + 20, BYTE_ORDER);
    }


    public static int quantityId()
    {
        return 5;
    }

    public static int quantitySinceVersion()
    {
        return 0;
    }

    public static int quantityEncodingOffset()
    {
        return 28;
    }

    public static int quantityEncodingLength()
    {
        return 8;
    }

    public static String quantityMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long quantityNullValue()
    {
        return -9223372036854775808L;
    }

    public static long quantityMinValue()
    {
        return -9223372036854775807L;
    }

    public static long quantityMaxValue()
    {
        return 9223372036854775807L;
    }

    public long quantity()
    {
        return buffer.getLong(offset + 28, BYTE_ORDER);
    }


    public static int buyerUserIdId()
    {
        return 6;
    }

    public static int buyerUserIdSinceVersion()
    {
        return 0;
    }

    public static int buyerUserIdEncodingOffset()
    {
        return 36;
    }

    public static int buyerUserIdEncodingLength()
    {
        return 8;
    }

    public static String buyerUserIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long buyerUserIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long buyerUserIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long buyerUserIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long buyerUserId()
    {
        return buffer.getLong(offset + 36, BYTE_ORDER);
    }


    public static int sellerUserIdId()
    {
        return 7;
    }

    public static int sellerUserIdSinceVersion()
    {
        return 0;
    }

    public static int sellerUserIdEncodingOffset()
    {
        return 44;
    }

    public static int sellerUserIdEncodingLength()
    {
        return 8;
    }

    public static String sellerUserIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long sellerUserIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long sellerUserIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long sellerUserIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long sellerUserId()
    {
        return buffer.getLong(offset + 44, BYTE_ORDER);
    }


    public static int takerIsBuyId()
    {
        return 8;
    }

    public static int takerIsBuySinceVersion()
    {
        return 0;
    }

    public static int takerIsBuyEncodingOffset()
    {
        return 52;
    }

    public static int takerIsBuyEncodingLength()
    {
        return 1;
    }

    public static String takerIsBuyMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short takerIsBuyRaw()
    {
        return ((short)(buffer.getByte(offset + 52) & 0xFF));
    }

    public BoolFlag takerIsBuy()
    {
        return BoolFlag.get(((short)(buffer.getByte(offset + 52) & 0xFF)));
    }


    public static int buyerBaseAfterId()
    {
        return 9;
    }

    public static int buyerBaseAfterSinceVersion()
    {
        return 0;
    }

    public static int buyerBaseAfterEncodingOffset()
    {
        return 53;
    }

    public static int buyerBaseAfterEncodingLength()
    {
        return 8;
    }

    public static String buyerBaseAfterMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long buyerBaseAfterNullValue()
    {
        return -9223372036854775808L;
    }

    public static long buyerBaseAfterMinValue()
    {
        return -9223372036854775807L;
    }

    public static long buyerBaseAfterMaxValue()
    {
        return 9223372036854775807L;
    }

    public long buyerBaseAfter()
    {
        return buffer.getLong(offset + 53, BYTE_ORDER);
    }


    public static int buyerQuoteAfterId()
    {
        return 10;
    }

    public static int buyerQuoteAfterSinceVersion()
    {
        return 0;
    }

    public static int buyerQuoteAfterEncodingOffset()
    {
        return 61;
    }

    public static int buyerQuoteAfterEncodingLength()
    {
        return 8;
    }

    public static String buyerQuoteAfterMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long buyerQuoteAfterNullValue()
    {
        return -9223372036854775808L;
    }

    public static long buyerQuoteAfterMinValue()
    {
        return -9223372036854775807L;
    }

    public static long buyerQuoteAfterMaxValue()
    {
        return 9223372036854775807L;
    }

    public long buyerQuoteAfter()
    {
        return buffer.getLong(offset + 61, BYTE_ORDER);
    }


    public static int sellerBaseAfterId()
    {
        return 11;
    }

    public static int sellerBaseAfterSinceVersion()
    {
        return 0;
    }

    public static int sellerBaseAfterEncodingOffset()
    {
        return 69;
    }

    public static int sellerBaseAfterEncodingLength()
    {
        return 8;
    }

    public static String sellerBaseAfterMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long sellerBaseAfterNullValue()
    {
        return -9223372036854775808L;
    }

    public static long sellerBaseAfterMinValue()
    {
        return -9223372036854775807L;
    }

    public static long sellerBaseAfterMaxValue()
    {
        return 9223372036854775807L;
    }

    public long sellerBaseAfter()
    {
        return buffer.getLong(offset + 69, BYTE_ORDER);
    }


    public static int sellerQuoteAfterId()
    {
        return 12;
    }

    public static int sellerQuoteAfterSinceVersion()
    {
        return 0;
    }

    public static int sellerQuoteAfterEncodingOffset()
    {
        return 77;
    }

    public static int sellerQuoteAfterEncodingLength()
    {
        return 8;
    }

    public static String sellerQuoteAfterMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long sellerQuoteAfterNullValue()
    {
        return -9223372036854775808L;
    }

    public static long sellerQuoteAfterMinValue()
    {
        return -9223372036854775807L;
    }

    public static long sellerQuoteAfterMaxValue()
    {
        return 9223372036854775807L;
    }

    public long sellerQuoteAfter()
    {
        return buffer.getLong(offset + 77, BYTE_ORDER);
    }


    public static int clusterTimeMsId()
    {
        return 13;
    }

    public static int clusterTimeMsSinceVersion()
    {
        return 0;
    }

    public static int clusterTimeMsEncodingOffset()
    {
        return 85;
    }

    public static int clusterTimeMsEncodingLength()
    {
        return 8;
    }

    public static String clusterTimeMsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long clusterTimeMsNullValue()
    {
        return -9223372036854775808L;
    }

    public static long clusterTimeMsMinValue()
    {
        return -9223372036854775807L;
    }

    public static long clusterTimeMsMaxValue()
    {
        return 9223372036854775807L;
    }

    public long clusterTimeMs()
    {
        return buffer.getLong(offset + 85, BYTE_ORDER);
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final JournalSettleDecoder decoder = new JournalSettleDecoder();
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
        builder.append("[JournalSettle](sbeTemplateId=");
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
        builder.append("journalSeq=");
        builder.append(this.journalSeq());
        builder.append('|');
        builder.append("tradeId=");
        builder.append(this.tradeId());
        builder.append('|');
        builder.append("marketId=");
        builder.append(this.marketId());
        builder.append('|');
        builder.append("price=");
        builder.append(this.price());
        builder.append('|');
        builder.append("quantity=");
        builder.append(this.quantity());
        builder.append('|');
        builder.append("buyerUserId=");
        builder.append(this.buyerUserId());
        builder.append('|');
        builder.append("sellerUserId=");
        builder.append(this.sellerUserId());
        builder.append('|');
        builder.append("takerIsBuy=");
        builder.append(this.takerIsBuy());
        builder.append('|');
        builder.append("buyerBaseAfter=");
        builder.append(this.buyerBaseAfter());
        builder.append('|');
        builder.append("buyerQuoteAfter=");
        builder.append(this.buyerQuoteAfter());
        builder.append('|');
        builder.append("sellerBaseAfter=");
        builder.append(this.sellerBaseAfter());
        builder.append('|');
        builder.append("sellerQuoteAfter=");
        builder.append(this.sellerQuoteAfter());
        builder.append('|');
        builder.append("clusterTimeMs=");
        builder.append(this.clusterTimeMs());

        limit(originalLimit);

        return builder;
    }
    
    public JournalSettleDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
