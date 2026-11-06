/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * BRIDGE: the bridge latches the feed (B 2.5). ORIGIN AdminAck{refTemplateId 66, ACCEPTED | DUPLICATE (stale)}; re-sent by the bridge on reconnect while halted. reason 0 (NONE) is a decode reject. Served by RC2 S7.
 */
@SuppressWarnings("all")
public final class FeedHaltEncoder
{
    public static final int BLOCK_LENGTH = 41;
    public static final int TEMPLATE_ID = 66;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final FeedHaltEncoder parentMessage = this;
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

    public FeedHaltEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public FeedHaltEncoder wrapAndApplyHeader(
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

    public FeedHaltEncoder correlationId(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
    }


    public static int reasonId()
    {
        return 2;
    }

    public static int reasonSinceVersion()
    {
        return 0;
    }

    public static int reasonEncodingOffset()
    {
        return 8;
    }

    public static int reasonEncodingLength()
    {
        return 1;
    }

    public static String reasonMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public FeedHaltEncoder reason(final FeedHaltReason value)
    {
        buffer.putByte(offset + 8, (byte)value.value());
        return this;
    }

    public static int meEpochSeenId()
    {
        return 3;
    }

    public static int meEpochSeenSinceVersion()
    {
        return 0;
    }

    public static int meEpochSeenEncodingOffset()
    {
        return 9;
    }

    public static int meEpochSeenEncodingLength()
    {
        return 8;
    }

    public static String meEpochSeenMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long meEpochSeenNullValue()
    {
        return -9223372036854775808L;
    }

    public static long meEpochSeenMinValue()
    {
        return -9223372036854775807L;
    }

    public static long meEpochSeenMaxValue()
    {
        return 9223372036854775807L;
    }

    public FeedHaltEncoder meEpochSeen(final long value)
    {
        buffer.putLong(offset + 9, value, BYTE_ORDER);
        return this;
    }


    public static int expectedJournalSeqId()
    {
        return 4;
    }

    public static int expectedJournalSeqSinceVersion()
    {
        return 0;
    }

    public static int expectedJournalSeqEncodingOffset()
    {
        return 17;
    }

    public static int expectedJournalSeqEncodingLength()
    {
        return 8;
    }

    public static String expectedJournalSeqMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long expectedJournalSeqNullValue()
    {
        return -9223372036854775808L;
    }

    public static long expectedJournalSeqMinValue()
    {
        return -9223372036854775807L;
    }

    public static long expectedJournalSeqMaxValue()
    {
        return 9223372036854775807L;
    }

    public FeedHaltEncoder expectedJournalSeq(final long value)
    {
        buffer.putLong(offset + 17, value, BYTE_ORDER);
        return this;
    }


    public static int seenJournalSeqId()
    {
        return 5;
    }

    public static int seenJournalSeqSinceVersion()
    {
        return 0;
    }

    public static int seenJournalSeqEncodingOffset()
    {
        return 25;
    }

    public static int seenJournalSeqEncodingLength()
    {
        return 8;
    }

    public static String seenJournalSeqMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long seenJournalSeqNullValue()
    {
        return -9223372036854775808L;
    }

    public static long seenJournalSeqMinValue()
    {
        return -9223372036854775807L;
    }

    public static long seenJournalSeqMaxValue()
    {
        return 9223372036854775807L;
    }

    public FeedHaltEncoder seenJournalSeq(final long value)
    {
        buffer.putLong(offset + 25, value, BYTE_ORDER);
        return this;
    }


    public static int bridgeIdentityId()
    {
        return 6;
    }

    public static int bridgeIdentitySinceVersion()
    {
        return 0;
    }

    public static int bridgeIdentityEncodingOffset()
    {
        return 33;
    }

    public static int bridgeIdentityEncodingLength()
    {
        return 8;
    }

    public static String bridgeIdentityMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long bridgeIdentityNullValue()
    {
        return -9223372036854775808L;
    }

    public static long bridgeIdentityMinValue()
    {
        return -9223372036854775807L;
    }

    public static long bridgeIdentityMaxValue()
    {
        return 9223372036854775807L;
    }

    public FeedHaltEncoder bridgeIdentity(final long value)
    {
        buffer.putLong(offset + 33, value, BYTE_ORDER);
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

        final FeedHaltDecoder decoder = new FeedHaltDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
