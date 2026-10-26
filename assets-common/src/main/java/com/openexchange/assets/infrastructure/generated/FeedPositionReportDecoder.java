/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * Answer to QueryFeedPosition: the ME journal cursor this AE has consumed
 */
@SuppressWarnings("all")
public final class FeedPositionReportDecoder
{
    public static final int BLOCK_LENGTH = 43;
    public static final int TEMPLATE_ID = 17;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final FeedPositionReportDecoder parentMessage = this;
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

    public FeedPositionReportDecoder wrap(
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

    public FeedPositionReportDecoder wrapAndApplyHeader(
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

    public FeedPositionReportDecoder sbeRewind()
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


    public static int consumePositionId()
    {
        return 2;
    }

    public static int consumePositionSinceVersion()
    {
        return 0;
    }

    public static int consumePositionEncodingOffset()
    {
        return 8;
    }

    public static int consumePositionEncodingLength()
    {
        return 8;
    }

    public static String consumePositionMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long consumePositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long consumePositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long consumePositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public long consumePosition()
    {
        return buffer.getLong(offset + 8, BYTE_ORDER);
    }


    public static int lastAppliedTradeIdId()
    {
        return 3;
    }

    public static int lastAppliedTradeIdSinceVersion()
    {
        return 0;
    }

    public static int lastAppliedTradeIdEncodingOffset()
    {
        return 16;
    }

    public static int lastAppliedTradeIdEncodingLength()
    {
        return 8;
    }

    public static String lastAppliedTradeIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long lastAppliedTradeIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long lastAppliedTradeIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long lastAppliedTradeIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long lastAppliedTradeId()
    {
        return buffer.getLong(offset + 16, BYTE_ORDER);
    }


    public static int journalEnabledId()
    {
        return 4;
    }

    public static int journalEnabledSinceVersion()
    {
        return 4;
    }

    public static int journalEnabledEncodingOffset()
    {
        return 24;
    }

    public static int journalEnabledEncodingLength()
    {
        return 1;
    }

    public static String journalEnabledMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short journalEnabledRaw()
    {
        if (parentMessage.actingVersion < 4)
        {
            return (short)255;
        }

        return ((short)(buffer.getByte(offset + 24) & 0xFF));
    }

    public BoolFlag journalEnabled()
    {
        if (parentMessage.actingVersion < 4)
        {
            return BoolFlag.NULL_VAL;
        }

        return BoolFlag.get(((short)(buffer.getByte(offset + 24) & 0xFF)));
    }


    public static int meEpochId()
    {
        return 5;
    }

    public static int meEpochSinceVersion()
    {
        return 7;
    }

    public static int meEpochEncodingOffset()
    {
        return 25;
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
        if (parentMessage.actingVersion < 7)
        {
            return -9223372036854775808L;
        }

        return buffer.getLong(offset + 25, BYTE_ORDER);
    }


    public static int lastAppliedJournalSeqId()
    {
        return 6;
    }

    public static int lastAppliedJournalSeqSinceVersion()
    {
        return 7;
    }

    public static int lastAppliedJournalSeqEncodingOffset()
    {
        return 33;
    }

    public static int lastAppliedJournalSeqEncodingLength()
    {
        return 8;
    }

    public static String lastAppliedJournalSeqMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long lastAppliedJournalSeqNullValue()
    {
        return -9223372036854775808L;
    }

    public static long lastAppliedJournalSeqMinValue()
    {
        return -9223372036854775807L;
    }

    public static long lastAppliedJournalSeqMaxValue()
    {
        return 9223372036854775807L;
    }

    public long lastAppliedJournalSeq()
    {
        if (parentMessage.actingVersion < 7)
        {
            return -9223372036854775808L;
        }

        return buffer.getLong(offset + 33, BYTE_ORDER);
    }


    public static int feedHaltedId()
    {
        return 7;
    }

    public static int feedHaltedSinceVersion()
    {
        return 7;
    }

    public static int feedHaltedEncodingOffset()
    {
        return 41;
    }

    public static int feedHaltedEncodingLength()
    {
        return 1;
    }

    public static String feedHaltedMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short feedHaltedRaw()
    {
        if (parentMessage.actingVersion < 7)
        {
            return (short)255;
        }

        return ((short)(buffer.getByte(offset + 41) & 0xFF));
    }

    public BoolFlag feedHalted()
    {
        if (parentMessage.actingVersion < 7)
        {
            return BoolFlag.NULL_VAL;
        }

        return BoolFlag.get(((short)(buffer.getByte(offset + 41) & 0xFF)));
    }


    public static int haltReasonId()
    {
        return 8;
    }

    public static int haltReasonSinceVersion()
    {
        return 7;
    }

    public static int haltReasonEncodingOffset()
    {
        return 42;
    }

    public static int haltReasonEncodingLength()
    {
        return 1;
    }

    public static String haltReasonMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short haltReasonRaw()
    {
        if (parentMessage.actingVersion < 7)
        {
            return (short)255;
        }

        return ((short)(buffer.getByte(offset + 42) & 0xFF));
    }

    public FeedHaltReason haltReason()
    {
        if (parentMessage.actingVersion < 7)
        {
            return FeedHaltReason.NULL_VAL;
        }

        return FeedHaltReason.get(((short)(buffer.getByte(offset + 42) & 0xFF)));
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final FeedPositionReportDecoder decoder = new FeedPositionReportDecoder();
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
        builder.append("[FeedPositionReport](sbeTemplateId=");
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
        builder.append("consumePosition=");
        builder.append(this.consumePosition());
        builder.append('|');
        builder.append("lastAppliedTradeId=");
        builder.append(this.lastAppliedTradeId());
        builder.append('|');
        builder.append("journalEnabled=");
        builder.append(this.journalEnabled());
        builder.append('|');
        builder.append("meEpoch=");
        builder.append(this.meEpoch());
        builder.append('|');
        builder.append("lastAppliedJournalSeq=");
        builder.append(this.lastAppliedJournalSeq());
        builder.append('|');
        builder.append("feedHalted=");
        builder.append(this.feedHalted());
        builder.append('|');
        builder.append("haltReason=");
        builder.append(this.haltReason());

        limit(originalLimit);

        return builder;
    }
    
    public FeedPositionReportDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
