/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * A run of MeJournalTrade entries (cap 64), applied strictly in group order; the bridge flushes it before any terminal or outcome.
 */
@SuppressWarnings("all")
public final class MeJournalTradeBatchDecoder
{
    public static final int BLOCK_LENGTH = 0;
    public static final int TEMPLATE_ID = 62;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final MeJournalTradeBatchDecoder parentMessage = this;
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

    public MeJournalTradeBatchDecoder wrap(
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

    public MeJournalTradeBatchDecoder wrapAndApplyHeader(
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

    public MeJournalTradeBatchDecoder sbeRewind()
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

    private final TradesDecoder trades = new TradesDecoder(this);

    public static long tradesDecoderId()
    {
        return 20;
    }

    public static int tradesDecoderSinceVersion()
    {
        return 0;
    }

    public TradesDecoder trades()
    {
        trades.wrap(buffer);
        return trades;
    }

    public static final class TradesDecoder
        implements Iterable<TradesDecoder>, java.util.Iterator<TradesDecoder>
    {
        public static final int HEADER_SIZE = 4;
        private final MeJournalTradeBatchDecoder parentMessage;
        private DirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int blockLength;

        TradesDecoder(final MeJournalTradeBatchDecoder parentMessage)
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

        public TradesDecoder next()
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
            return 109;
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

        public java.util.Iterator<TradesDecoder> iterator()
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

        public long egressSeq()
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

        public long takerOrderId()
        {
            return buffer.getLong(offset + 20, BYTE_ORDER);
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

        public long takerUserId()
        {
            return buffer.getLong(offset + 28, BYTE_ORDER);
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

        public long makerOrderId()
        {
            return buffer.getLong(offset + 36, BYTE_ORDER);
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

        public long makerUserId()
        {
            return buffer.getLong(offset + 44, BYTE_ORDER);
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

        public long price()
        {
            return buffer.getLong(offset + 52, BYTE_ORDER);
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

        public long quantity()
        {
            return buffer.getLong(offset + 60, BYTE_ORDER);
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

        public short takerIsBuyRaw()
        {
            return ((short)(buffer.getByte(offset + 68) & 0xFF));
        }

        public BoolFlag takerIsBuy()
        {
            return BoolFlag.get(((short)(buffer.getByte(offset + 68) & 0xFF)));
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

        public long meTimestamp()
        {
            return buffer.getLong(offset + 69, BYTE_ORDER);
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

        public long takerOmsOrderId()
        {
            return buffer.getLong(offset + 77, BYTE_ORDER);
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

        public long makerOmsOrderId()
        {
            return buffer.getLong(offset + 85, BYTE_ORDER);
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

        public long meEpoch()
        {
            return buffer.getLong(offset + 93, BYTE_ORDER);
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

        public long journalSeq()
        {
            if (parentMessage.actingVersion < 7)
            {
                return -9223372036854775808L;
            }

            return buffer.getLong(offset + 101, BYTE_ORDER);
        }


        public StringBuilder appendTo(final StringBuilder builder)
        {
            if (null == buffer)
            {
                return builder;
            }

            builder.append('(');
            builder.append("egressSeq=");
            builder.append(this.egressSeq());
            builder.append('|');
            builder.append("tradeId=");
            builder.append(this.tradeId());
            builder.append('|');
            builder.append("marketId=");
            builder.append(this.marketId());
            builder.append('|');
            builder.append("takerOrderId=");
            builder.append(this.takerOrderId());
            builder.append('|');
            builder.append("takerUserId=");
            builder.append(this.takerUserId());
            builder.append('|');
            builder.append("makerOrderId=");
            builder.append(this.makerOrderId());
            builder.append('|');
            builder.append("makerUserId=");
            builder.append(this.makerUserId());
            builder.append('|');
            builder.append("price=");
            builder.append(this.price());
            builder.append('|');
            builder.append("quantity=");
            builder.append(this.quantity());
            builder.append('|');
            builder.append("takerIsBuy=");
            builder.append(this.takerIsBuy());
            builder.append('|');
            builder.append("meTimestamp=");
            builder.append(this.meTimestamp());
            builder.append('|');
            builder.append("takerOmsOrderId=");
            builder.append(this.takerOmsOrderId());
            builder.append('|');
            builder.append("makerOmsOrderId=");
            builder.append(this.makerOmsOrderId());
            builder.append('|');
            builder.append("meEpoch=");
            builder.append(this.meEpoch());
            builder.append('|');
            builder.append("journalSeq=");
            builder.append(this.journalSeq());
            builder.append(')');

            return builder;
        }
        
        public TradesDecoder sbeSkip()
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

        final MeJournalTradeBatchDecoder decoder = new MeJournalTradeBatchDecoder();
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
        builder.append("[MeJournalTradeBatch](sbeTemplateId=");
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
        builder.append("trades=[");
        final int tradesOriginalOffset = trades.offset;
        final int tradesOriginalIndex = trades.index;
        final TradesDecoder trades = this.trades();
        if (trades.count() > 0)
        {
            while (trades.hasNext())
            {
                trades.next().appendTo(builder);
                builder.append(',');
            }
            builder.setLength(builder.length() - 1);
        }
        trades.offset = tradesOriginalOffset;
        trades.index = tradesOriginalIndex;
        builder.append(']');

        limit(originalLimit);

        return builder;
    }
    
    public MeJournalTradeBatchDecoder sbeSkip()
    {
        sbeRewind();
        TradesDecoder trades = this.trades();
        if (trades.count() > 0)
        {
            while (trades.hasNext())
            {
                trades.next();
                trades.sbeSkip();
            }
        }

        return this;
    }
}
