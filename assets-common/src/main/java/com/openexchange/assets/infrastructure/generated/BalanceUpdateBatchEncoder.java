/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * A run of BalanceUpdate events (id=12), in emission order
 */
@SuppressWarnings("all")
public final class BalanceUpdateBatchEncoder
{
    public static final int BLOCK_LENGTH = 0;
    public static final int TEMPLATE_ID = 22;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final BalanceUpdateBatchEncoder parentMessage = this;
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

    public BalanceUpdateBatchEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public BalanceUpdateBatchEncoder wrapAndApplyHeader(
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

    private final UpdatesEncoder updates = new UpdatesEncoder(this);

    public static long updatesId()
    {
        return 10;
    }

    public UpdatesEncoder updatesCount(final int count)
    {
        updates.wrap(buffer, count);
        return updates;
    }

    public static final class UpdatesEncoder
    {
        public static final int HEADER_SIZE = 4;
        private final BalanceUpdateBatchEncoder parentMessage;
        private MutableDirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int initialLimit;

        UpdatesEncoder(final BalanceUpdateBatchEncoder parentMessage)
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
            buffer.putShort(limit + 0, (short)28, BYTE_ORDER);
            buffer.putShort(limit + 2, (short)count, BYTE_ORDER);
        }

        public UpdatesEncoder next()
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
            return 28;
        }

        public static int userIdId()
        {
            return 1;
        }

        public static int userIdSinceVersion()
        {
            return 0;
        }

        public static int userIdEncodingOffset()
        {
            return 0;
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

        public UpdatesEncoder userId(final long value)
        {
            buffer.putLong(offset + 0, value, BYTE_ORDER);
            return this;
        }


        public static int assetIdId()
        {
            return 2;
        }

        public static int assetIdSinceVersion()
        {
            return 0;
        }

        public static int assetIdEncodingOffset()
        {
            return 8;
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

        public UpdatesEncoder assetId(final int value)
        {
            buffer.putInt(offset + 8, value, BYTE_ORDER);
            return this;
        }


        public static int availableId()
        {
            return 3;
        }

        public static int availableSinceVersion()
        {
            return 0;
        }

        public static int availableEncodingOffset()
        {
            return 12;
        }

        public static int availableEncodingLength()
        {
            return 8;
        }

        public static String availableMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long availableNullValue()
        {
            return -9223372036854775808L;
        }

        public static long availableMinValue()
        {
            return -9223372036854775807L;
        }

        public static long availableMaxValue()
        {
            return 9223372036854775807L;
        }

        public UpdatesEncoder available(final long value)
        {
            buffer.putLong(offset + 12, value, BYTE_ORDER);
            return this;
        }


        public static int lockedId()
        {
            return 4;
        }

        public static int lockedSinceVersion()
        {
            return 0;
        }

        public static int lockedEncodingOffset()
        {
            return 20;
        }

        public static int lockedEncodingLength()
        {
            return 8;
        }

        public static String lockedMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long lockedNullValue()
        {
            return -9223372036854775808L;
        }

        public static long lockedMinValue()
        {
            return -9223372036854775807L;
        }

        public static long lockedMaxValue()
        {
            return 9223372036854775807L;
        }

        public UpdatesEncoder locked(final long value)
        {
            buffer.putLong(offset + 20, value, BYTE_ORDER);
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

        final BalanceUpdateBatchDecoder decoder = new BalanceUpdateBatchDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
