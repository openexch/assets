/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * A run of HoldSnapshotEntry events (id=19) answering RequestHoldSnapshot; HoldSnapshotEnd (id=20) still terminates the stream
 */
@SuppressWarnings("all")
public final class HoldSnapshotBatchDecoder
{
    public static final int BLOCK_LENGTH = 0;
    public static final int TEMPLATE_ID = 25;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final HoldSnapshotBatchDecoder parentMessage = this;
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

    public HoldSnapshotBatchDecoder wrap(
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

    public HoldSnapshotBatchDecoder wrapAndApplyHeader(
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

    public HoldSnapshotBatchDecoder sbeRewind()
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

    private final HoldsDecoder holds = new HoldsDecoder(this);

    public static long holdsDecoderId()
    {
        return 10;
    }

    public static int holdsDecoderSinceVersion()
    {
        return 0;
    }

    public HoldsDecoder holds()
    {
        holds.wrap(buffer);
        return holds;
    }

    public static final class HoldsDecoder
        implements Iterable<HoldsDecoder>, java.util.Iterator<HoldsDecoder>
    {
        public static final int HEADER_SIZE = 4;
        private final HoldSnapshotBatchDecoder parentMessage;
        private DirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int blockLength;

        HoldsDecoder(final HoldSnapshotBatchDecoder parentMessage)
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

        public HoldsDecoder next()
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
            return 28;
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

        public java.util.Iterator<HoldsDecoder> iterator()
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

        public static int orderIdId()
        {
            return 1;
        }

        public static int orderIdSinceVersion()
        {
            return 0;
        }

        public static int orderIdEncodingOffset()
        {
            return 0;
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
            return buffer.getLong(offset + 0, BYTE_ORDER);
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

        public long userId()
        {
            return buffer.getLong(offset + 8, BYTE_ORDER);
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

        public int assetId()
        {
            return buffer.getInt(offset + 16, BYTE_ORDER);
        }


        public static int remainingId()
        {
            return 4;
        }

        public static int remainingSinceVersion()
        {
            return 0;
        }

        public static int remainingEncodingOffset()
        {
            return 20;
        }

        public static int remainingEncodingLength()
        {
            return 8;
        }

        public static String remainingMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long remainingNullValue()
        {
            return -9223372036854775808L;
        }

        public static long remainingMinValue()
        {
            return -9223372036854775807L;
        }

        public static long remainingMaxValue()
        {
            return 9223372036854775807L;
        }

        public long remaining()
        {
            return buffer.getLong(offset + 20, BYTE_ORDER);
        }


        public StringBuilder appendTo(final StringBuilder builder)
        {
            if (null == buffer)
            {
                return builder;
            }

            builder.append('(');
            builder.append("orderId=");
            builder.append(this.orderId());
            builder.append('|');
            builder.append("userId=");
            builder.append(this.userId());
            builder.append('|');
            builder.append("assetId=");
            builder.append(this.assetId());
            builder.append('|');
            builder.append("remaining=");
            builder.append(this.remaining());
            builder.append(')');

            return builder;
        }
        
        public HoldsDecoder sbeSkip()
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

        final HoldSnapshotBatchDecoder decoder = new HoldSnapshotBatchDecoder();
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
        builder.append("[HoldSnapshotBatch](sbeTemplateId=");
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
        builder.append("holds=[");
        final int holdsOriginalOffset = holds.offset;
        final int holdsOriginalIndex = holds.index;
        final HoldsDecoder holds = this.holds();
        if (holds.count() > 0)
        {
            while (holds.hasNext())
            {
                holds.next().appendTo(builder);
                builder.append(',');
            }
            builder.setLength(builder.length() - 1);
        }
        holds.offset = holdsOriginalOffset;
        holds.index = holdsOriginalIndex;
        builder.append(']');

        limit(originalLimit);

        return builder;
    }
    
    public HoldSnapshotBatchDecoder sbeSkip()
    {
        sbeRewind();
        HoldsDecoder holds = this.holds();
        if (holds.count() > 0)
        {
            while (holds.hasNext())
            {
                holds.next();
                holds.sbeSkip();
            }
        }

        return this;
    }
}
