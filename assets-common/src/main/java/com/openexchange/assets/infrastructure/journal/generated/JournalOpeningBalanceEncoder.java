/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.journal.generated;

import org.agrona.MutableDirectBuffer;


/**
 * Opening-balance epoch row: one per (user, asset) with a nonzero available+locked total at the instant journaling first engages on a non-empty ledger, emitted in ascending (userId, assetId) order BEFORE the first journaled movement. amount = available + locked total. On an empty ledger the epoch is implicit (the journal starts from nothing).
 */
@SuppressWarnings("all")
public final class JournalOpeningBalanceEncoder
{
    public static final int BLOCK_LENGTH = 28;
    public static final int TEMPLATE_ID = 1;
    public static final int SCHEMA_ID = 3;
    public static final int SCHEMA_VERSION = 1;
    public static final String SEMANTIC_VERSION = "0.1";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final JournalOpeningBalanceEncoder parentMessage = this;
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

    public JournalOpeningBalanceEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public JournalOpeningBalanceEncoder wrapAndApplyHeader(
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

    public JournalOpeningBalanceEncoder journalSeq(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
    }


    public static int userIdId()
    {
        return 2;
    }

    public static int userIdSinceVersion()
    {
        return 0;
    }

    public static int userIdEncodingOffset()
    {
        return 8;
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

    public JournalOpeningBalanceEncoder userId(final long value)
    {
        buffer.putLong(offset + 8, value, BYTE_ORDER);
        return this;
    }


    public static int assetIdId()
    {
        return 3;
    }

    public static int assetIdSinceVersion()
    {
        return 0;
    }

    public static int assetIdEncodingOffset()
    {
        return 16;
    }

    public static int assetIdEncodingLength()
    {
        return 4;
    }

    public static String assetIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int assetIdNullValue()
    {
        return -2147483648;
    }

    public static int assetIdMinValue()
    {
        return -2147483647;
    }

    public static int assetIdMaxValue()
    {
        return 2147483647;
    }

    public JournalOpeningBalanceEncoder assetId(final int value)
    {
        buffer.putInt(offset + 16, value, BYTE_ORDER);
        return this;
    }


    public static int amountId()
    {
        return 4;
    }

    public static int amountSinceVersion()
    {
        return 0;
    }

    public static int amountEncodingOffset()
    {
        return 20;
    }

    public static int amountEncodingLength()
    {
        return 8;
    }

    public static String amountMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long amountNullValue()
    {
        return -9223372036854775808L;
    }

    public static long amountMinValue()
    {
        return -9223372036854775807L;
    }

    public static long amountMaxValue()
    {
        return 9223372036854775807L;
    }

    public JournalOpeningBalanceEncoder amount(final long value)
    {
        buffer.putLong(offset + 20, value, BYTE_ORDER);
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

        final JournalOpeningBalanceDecoder decoder = new JournalOpeningBalanceDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
