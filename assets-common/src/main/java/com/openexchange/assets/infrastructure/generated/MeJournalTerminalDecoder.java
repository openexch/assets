/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * ME journal terminal (status 2/3/4 only), with the status and marketId the v5 bridge dropped. Hold-scoped residual release + the order-state transition.
 */
@SuppressWarnings("all")
public final class MeJournalTerminalDecoder
{
    public static final int BLOCK_LENGTH = 62;
    public static final int TEMPLATE_ID = 63;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final MeJournalTerminalDecoder parentMessage = this;
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

    public MeJournalTerminalDecoder wrap(
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

    public MeJournalTerminalDecoder wrapAndApplyHeader(
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

    public MeJournalTerminalDecoder sbeRewind()
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


    public static int orderIdId()
    {
        return 2;
    }

    public static int orderIdSinceVersion()
    {
        return 0;
    }

    public static int orderIdEncodingOffset()
    {
        return 8;
    }

    public static int orderIdEncodingLength()
    {
        return 8;
    }

    public static String orderIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long orderIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long orderIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long orderIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long orderId()
    {
        return buffer.getLong(offset + 8, BYTE_ORDER);
    }


    public static int userIdId()
    {
        return 3;
    }

    public static int userIdSinceVersion()
    {
        return 0;
    }

    public static int userIdEncodingOffset()
    {
        return 16;
    }

    public static int userIdEncodingLength()
    {
        return 8;
    }

    public static String userIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long userIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long userIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long userIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long userId()
    {
        return buffer.getLong(offset + 16, BYTE_ORDER);
    }


    public static int marketIdId()
    {
        return 4;
    }

    public static int marketIdSinceVersion()
    {
        return 0;
    }

    public static int marketIdEncodingOffset()
    {
        return 24;
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
        return buffer.getInt(offset + 24, BYTE_ORDER);
    }


    public static int statusId()
    {
        return 5;
    }

    public static int statusSinceVersion()
    {
        return 0;
    }

    public static int statusEncodingOffset()
    {
        return 28;
    }

    public static int statusEncodingLength()
    {
        return 1;
    }

    public static String statusMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short statusRaw()
    {
        return ((short)(buffer.getByte(offset + 28) & 0xFF));
    }

    public OrderStatus status()
    {
        return OrderStatus.get(((short)(buffer.getByte(offset + 28) & 0xFF)));
    }


    public static int meTimestampId()
    {
        return 6;
    }

    public static int meTimestampSinceVersion()
    {
        return 0;
    }

    public static int meTimestampEncodingOffset()
    {
        return 29;
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
        return buffer.getLong(offset + 29, BYTE_ORDER);
    }


    public static int omsOrderIdId()
    {
        return 7;
    }

    public static int omsOrderIdSinceVersion()
    {
        return 0;
    }

    public static int omsOrderIdEncodingOffset()
    {
        return 37;
    }

    public static int omsOrderIdEncodingLength()
    {
        return 8;
    }

    public static String omsOrderIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long omsOrderIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long omsOrderIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long omsOrderIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long omsOrderId()
    {
        return buffer.getLong(offset + 37, BYTE_ORDER);
    }


    public static int meEpochId()
    {
        return 8;
    }

    public static int meEpochSinceVersion()
    {
        return 0;
    }

    public static int meEpochEncodingOffset()
    {
        return 45;
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
        return buffer.getLong(offset + 45, BYTE_ORDER);
    }


    public static int journalSeqId()
    {
        return 9;
    }

    public static int journalSeqSinceVersion()
    {
        return 7;
    }

    public static int journalSeqEncodingOffset()
    {
        return 53;
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

        return buffer.getLong(offset + 53, BYTE_ORDER);
    }


    public static int rejectReasonId()
    {
        return 10;
    }

    public static int rejectReasonSinceVersion()
    {
        return 7;
    }

    public static int rejectReasonEncodingOffset()
    {
        return 61;
    }

    public static int rejectReasonEncodingLength()
    {
        return 1;
    }

    public static String rejectReasonMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static short rejectReasonNullValue()
    {
        return (short)255;
    }

    public static short rejectReasonMinValue()
    {
        return (short)0;
    }

    public static short rejectReasonMaxValue()
    {
        return (short)254;
    }

    public short rejectReason()
    {
        if (parentMessage.actingVersion < 7)
        {
            return (short)255;
        }

        return ((short)(buffer.getByte(offset + 61) & 0xFF));
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final MeJournalTerminalDecoder decoder = new MeJournalTerminalDecoder();
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
        builder.append("[MeJournalTerminal](sbeTemplateId=");
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
        builder.append("egressSeq=");
        builder.append(this.egressSeq());
        builder.append('|');
        builder.append("orderId=");
        builder.append(this.orderId());
        builder.append('|');
        builder.append("userId=");
        builder.append(this.userId());
        builder.append('|');
        builder.append("marketId=");
        builder.append(this.marketId());
        builder.append('|');
        builder.append("status=");
        builder.append(this.status());
        builder.append('|');
        builder.append("meTimestamp=");
        builder.append(this.meTimestamp());
        builder.append('|');
        builder.append("omsOrderId=");
        builder.append(this.omsOrderId());
        builder.append('|');
        builder.append("meEpoch=");
        builder.append(this.meEpoch());
        builder.append('|');
        builder.append("journalSeq=");
        builder.append(this.journalSeq());
        builder.append('|');
        builder.append("rejectReason=");
        builder.append(this.rejectReason());

        limit(originalLimit);

        return builder;
    }
    
    public MeJournalTerminalDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
