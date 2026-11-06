/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * BRIDGE: the bridge latches the feed (B 2.5). ORIGIN AdminAck{refTemplateId 66, ACCEPTED | DUPLICATE (stale)}; re-sent by the bridge on reconnect while halted. reason 0 (NONE) is a decode reject. Served by RC2 S7.
 */
@SuppressWarnings("all")
public final class FeedHaltDecoder
{
    public static final int BLOCK_LENGTH = 41;
    public static final int TEMPLATE_ID = 66;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final FeedHaltDecoder parentMessage = this;
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

    public FeedHaltDecoder wrap(
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

    public FeedHaltDecoder wrapAndApplyHeader(
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

    public FeedHaltDecoder sbeRewind()
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

    public short reasonRaw()
    {
        return ((short)(buffer.getByte(offset + 8) & 0xFF));
    }

    public FeedHaltReason reason()
    {
        return FeedHaltReason.get(((short)(buffer.getByte(offset + 8) & 0xFF)));
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

    public long meEpochSeen()
    {
        return buffer.getLong(offset + 9, BYTE_ORDER);
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

    public long expectedJournalSeq()
    {
        return buffer.getLong(offset + 17, BYTE_ORDER);
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

    public long seenJournalSeq()
    {
        return buffer.getLong(offset + 25, BYTE_ORDER);
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

    public long bridgeIdentity()
    {
        return buffer.getLong(offset + 33, BYTE_ORDER);
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final FeedHaltDecoder decoder = new FeedHaltDecoder();
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
        builder.append("[FeedHalt](sbeTemplateId=");
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
        builder.append("reason=");
        builder.append(this.reason());
        builder.append('|');
        builder.append("meEpochSeen=");
        builder.append(this.meEpochSeen());
        builder.append('|');
        builder.append("expectedJournalSeq=");
        builder.append(this.expectedJournalSeq());
        builder.append('|');
        builder.append("seenJournalSeq=");
        builder.append(this.seenJournalSeq());
        builder.append('|');
        builder.append("bridgeIdentity=");
        builder.append(this.bridgeIdentity());

        limit(originalLimit);

        return builder;
    }
    
    public FeedHaltDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
