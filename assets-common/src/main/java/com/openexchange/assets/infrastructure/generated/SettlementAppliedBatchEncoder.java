/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * A run of SettlementApplied events (id=13), in emission order
 */
@SuppressWarnings("all")
public final class SettlementAppliedBatchEncoder
{
    public static final int BLOCK_LENGTH = 0;
    public static final int TEMPLATE_ID = 24;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 5;
    public static final String SEMANTIC_VERSION = "0.5";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final SettlementAppliedBatchEncoder parentMessage = this;
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

    public SettlementAppliedBatchEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public SettlementAppliedBatchEncoder wrapAndApplyHeader(
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

    private final SettlementsEncoder settlements = new SettlementsEncoder(this);

    public static long settlementsId()
    {
        return 10;
    }

    public SettlementsEncoder settlementsCount(final int count)
    {
        settlements.wrap(buffer, count);
        return settlements;
    }

    public static final class SettlementsEncoder
    {
        public static final int HEADER_SIZE = 4;
        private final SettlementAppliedBatchEncoder parentMessage;
        private MutableDirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int initialLimit;

        SettlementsEncoder(final SettlementAppliedBatchEncoder parentMessage)
        {
            this.parentMessage = parentMessage;
        }

        public void wrap(final MutableDirectBuffer buffer, final int count)
        {
            if (count < 0 || count > 65534)
            {
                throw new IllegalArgumentException("count outside allowed range: count=" + count);
            }

            if (buffer != this.buffer)
            {
                this.buffer = buffer;
            }

            index = 0;
            this.count = count;
            final int limit = parentMessage.limit();
            initialLimit = limit;
            parentMessage.limit(limit + HEADER_SIZE);
            buffer.putShort(limit + 0, (short)24, BYTE_ORDER);
            buffer.putShort(limit + 2, (short)count, BYTE_ORDER);
        }

        public SettlementsEncoder next()
        {
            if (index >= count)
            {
                throw new java.util.NoSuchElementException();
            }

            offset = parentMessage.limit();
            parentMessage.limit(offset + sbeBlockLength());
            ++index;

            return this;
        }

        public int resetCountToIndex()
        {
            count = index;
            buffer.putShort(initialLimit + 2, (short)count, BYTE_ORDER);

            return count;
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
            return 24;
        }

        public static int tradeIdId()
        {
            return 1;
        }

        public static int tradeIdSinceVersion()
        {
            return 0;
        }

        public static int tradeIdEncodingOffset()
        {
            return 0;
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

        public SettlementsEncoder tradeId(final long value)
        {
            buffer.putLong(offset + 0, value, BYTE_ORDER);
            return this;
        }


        public static int buyerUserIdId()
        {
            return 2;
        }

        public static int buyerUserIdSinceVersion()
        {
            return 0;
        }

        public static int buyerUserIdEncodingOffset()
        {
            return 8;
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

        public SettlementsEncoder buyerUserId(final long value)
        {
            buffer.putLong(offset + 8, value, BYTE_ORDER);
            return this;
        }


        public static int sellerUserIdId()
        {
            return 3;
        }

        public static int sellerUserIdSinceVersion()
        {
            return 0;
        }

        public static int sellerUserIdEncodingOffset()
        {
            return 16;
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

        public SettlementsEncoder sellerUserId(final long value)
        {
            buffer.putLong(offset + 16, value, BYTE_ORDER);
            return this;
        }

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

        final SettlementAppliedBatchDecoder decoder = new SettlementAppliedBatchDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
