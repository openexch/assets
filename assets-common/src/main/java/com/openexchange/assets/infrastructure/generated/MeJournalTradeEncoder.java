/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * Every JournalTrade field from the ME settlement journal (the v5 bridge dropped the two cluster leg ids and the timestamp) plus the producer epoch (R03 IN-08). Dense tradeId: at or below the high-water = no-op; a gap = reject + gate.
 */
@SuppressWarnings("all")
public final class MeJournalTradeEncoder
{
    public static final int BLOCK_LENGTH = 109;
    public static final int TEMPLATE_ID = 61;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final MeJournalTradeEncoder parentMessage = this;
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

    public MeJournalTradeEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public MeJournalTradeEncoder wrapAndApplyHeader(
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

    public static int egressSeqId()
    {
        return 1;
    }

    public static int egressSeqSinceVersion()
    {
        return 0;
    }

    public static int egressSeqEncodingOffset()
    {
        return 0;
    }

    public static int egressSeqEncodingLength()
    {
        return 8;
    }

    public static String egressSeqMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long egressSeqNullValue()
    {
        return -9223372036854775808L;
    }

    public static long egressSeqMinValue()
    {
        return -9223372036854775807L;
    }

    public static long egressSeqMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeJournalTradeEncoder egressSeq(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
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

    public MeJournalTradeEncoder tradeId(final long value)
    {
        buffer.putLong(offset + 8, value, BYTE_ORDER);
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

    public MeJournalTradeEncoder marketId(final int value)
    {
        buffer.putInt(offset + 16, value, BYTE_ORDER);
        return this;
    }


    public static int takerOrderIdId()
    {
        return 4;
    }

    public static int takerOrderIdSinceVersion()
    {
        return 0;
    }

    public static int takerOrderIdEncodingOffset()
    {
        return 20;
    }

    public static int takerOrderIdEncodingLength()
    {
        return 8;
    }

    public static String takerOrderIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long takerOrderIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long takerOrderIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long takerOrderIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeJournalTradeEncoder takerOrderId(final long value)
    {
        buffer.putLong(offset + 20, value, BYTE_ORDER);
        return this;
    }


    public static int takerUserIdId()
    {
        return 5;
    }

    public static int takerUserIdSinceVersion()
    {
        return 0;
    }

    public static int takerUserIdEncodingOffset()
    {
        return 28;
    }

    public static int takerUserIdEncodingLength()
    {
        return 8;
    }

    public static String takerUserIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long takerUserIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long takerUserIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long takerUserIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeJournalTradeEncoder takerUserId(final long value)
    {
        buffer.putLong(offset + 28, value, BYTE_ORDER);
        return this;
    }


    public static int makerOrderIdId()
    {
        return 6;
    }

    public static int makerOrderIdSinceVersion()
    {
        return 0;
    }

    public static int makerOrderIdEncodingOffset()
    {
        return 36;
    }

    public static int makerOrderIdEncodingLength()
    {
        return 8;
    }

    public static String makerOrderIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long makerOrderIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long makerOrderIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long makerOrderIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeJournalTradeEncoder makerOrderId(final long value)
    {
        buffer.putLong(offset + 36, value, BYTE_ORDER);
        return this;
    }


    public static int makerUserIdId()
    {
        return 7;
    }

    public static int makerUserIdSinceVersion()
    {
        return 0;
    }

    public static int makerUserIdEncodingOffset()
    {
        return 44;
    }

    public static int makerUserIdEncodingLength()
    {
        return 8;
    }

    public static String makerUserIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long makerUserIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long makerUserIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long makerUserIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeJournalTradeEncoder makerUserId(final long value)
    {
        buffer.putLong(offset + 44, value, BYTE_ORDER);
        return this;
    }


    public static int priceId()
    {
        return 8;
    }

    public static int priceSinceVersion()
    {
        return 0;
    }

    public static int priceEncodingOffset()
    {
        return 52;
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

    public MeJournalTradeEncoder price(final long value)
    {
        buffer.putLong(offset + 52, value, BYTE_ORDER);
        return this;
    }


    public static int quantityId()
    {
        return 9;
    }

    public static int quantitySinceVersion()
    {
        return 0;
    }

    public static int quantityEncodingOffset()
    {
        return 60;
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

    public MeJournalTradeEncoder quantity(final long value)
    {
        buffer.putLong(offset + 60, value, BYTE_ORDER);
        return this;
    }


    public static int takerIsBuyId()
    {
        return 10;
    }

    public static int takerIsBuySinceVersion()
    {
        return 0;
    }

    public static int takerIsBuyEncodingOffset()
    {
        return 68;
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

    public MeJournalTradeEncoder takerIsBuy(final BoolFlag value)
    {
        buffer.putByte(offset + 68, (byte)value.value());
        return this;
    }

    public static int meTimestampId()
    {
        return 11;
    }

    public static int meTimestampSinceVersion()
    {
        return 0;
    }

    public static int meTimestampEncodingOffset()
    {
        return 69;
    }

    public static int meTimestampEncodingLength()
    {
        return 8;
    }

    public static String meTimestampMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long meTimestampNullValue()
    {
        return -9223372036854775808L;
    }

    public static long meTimestampMinValue()
    {
        return -9223372036854775807L;
    }

    public static long meTimestampMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeJournalTradeEncoder meTimestamp(final long value)
    {
        buffer.putLong(offset + 69, value, BYTE_ORDER);
        return this;
    }


    public static int takerOmsOrderIdId()
    {
        return 12;
    }

    public static int takerOmsOrderIdSinceVersion()
    {
        return 0;
    }

    public static int takerOmsOrderIdEncodingOffset()
    {
        return 77;
    }

    public static int takerOmsOrderIdEncodingLength()
    {
        return 8;
    }

    public static String takerOmsOrderIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long takerOmsOrderIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long takerOmsOrderIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long takerOmsOrderIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeJournalTradeEncoder takerOmsOrderId(final long value)
    {
        buffer.putLong(offset + 77, value, BYTE_ORDER);
        return this;
    }


    public static int makerOmsOrderIdId()
    {
        return 13;
    }

    public static int makerOmsOrderIdSinceVersion()
    {
        return 0;
    }

    public static int makerOmsOrderIdEncodingOffset()
    {
        return 85;
    }

    public static int makerOmsOrderIdEncodingLength()
    {
        return 8;
    }

    public static String makerOmsOrderIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long makerOmsOrderIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long makerOmsOrderIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long makerOmsOrderIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeJournalTradeEncoder makerOmsOrderId(final long value)
    {
        buffer.putLong(offset + 85, value, BYTE_ORDER);
        return this;
    }


    public static int meEpochId()
    {
        return 14;
    }

    public static int meEpochSinceVersion()
    {
        return 0;
    }

    public static int meEpochEncodingOffset()
    {
        return 93;
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

    public MeJournalTradeEncoder meEpoch(final long value)
    {
        buffer.putLong(offset + 93, value, BYTE_ORDER);
        return this;
    }


    public static int journalSeqId()
    {
        return 15;
    }

    public static int journalSeqSinceVersion()
    {
        return 7;
    }

    public static int journalSeqEncodingOffset()
    {
        return 101;
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

    public MeJournalTradeEncoder journalSeq(final long value)
    {
        buffer.putLong(offset + 101, value, BYTE_ORDER);
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

        final MeJournalTradeDecoder decoder = new MeJournalTradeDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
