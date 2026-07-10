/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * A settle leg could not draw fully from its order hold (reconciler race / invariant breach): drawnFromAvailable was recovered from the user's available balance; uncovered could not be moved at all -- the counterparty was credited only what was debited, so conservation holds; nonzero uncovered demands operator action. MUST alert on this event.
 */
@SuppressWarnings("all")
public final class SettleFaultEncoder
{
    public static final int BLOCK_LENGTH = 44;
    public static final int TEMPLATE_ID = 21;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 2;
    public static final String SEMANTIC_VERSION = "0.2";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final SettleFaultEncoder parentMessage = this;
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

    public SettleFaultEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public SettleFaultEncoder wrapAndApplyHeader(
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

    public SettleFaultEncoder tradeId(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
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

    public SettleFaultEncoder orderId(final long value)
    {
        buffer.putLong(offset + 8, value, BYTE_ORDER);
        return this;
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

    public SettleFaultEncoder userId(final long value)
    {
        buffer.putLong(offset + 16, value, BYTE_ORDER);
        return this;
    }


    public static int assetIdId()
    {
        return 4;
    }

    public static int assetIdSinceVersion()
    {
        return 0;
    }

    public static int assetIdEncodingOffset()
    {
        return 24;
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

    public SettleFaultEncoder assetId(final int value)
    {
        buffer.putInt(offset + 24, value, BYTE_ORDER);
        return this;
    }


    public static int drawnFromAvailableId()
    {
        return 5;
    }

    public static int drawnFromAvailableSinceVersion()
    {
        return 0;
    }

    public static int drawnFromAvailableEncodingOffset()
    {
        return 28;
    }

    public static int drawnFromAvailableEncodingLength()
    {
        return 8;
    }

    public static String drawnFromAvailableMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long drawnFromAvailableNullValue()
    {
        return -9223372036854775808L;
    }

    public static long drawnFromAvailableMinValue()
    {
        return -9223372036854775807L;
    }

    public static long drawnFromAvailableMaxValue()
    {
        return 9223372036854775807L;
    }

    public SettleFaultEncoder drawnFromAvailable(final long value)
    {
        buffer.putLong(offset + 28, value, BYTE_ORDER);
        return this;
    }


    public static int uncoveredId()
    {
        return 6;
    }

    public static int uncoveredSinceVersion()
    {
        return 0;
    }

    public static int uncoveredEncodingOffset()
    {
        return 36;
    }

    public static int uncoveredEncodingLength()
    {
        return 8;
    }

    public static String uncoveredMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long uncoveredNullValue()
    {
        return -9223372036854775808L;
    }

    public static long uncoveredMinValue()
    {
        return -9223372036854775807L;
    }

    public static long uncoveredMaxValue()
    {
        return 9223372036854775807L;
    }

    public SettleFaultEncoder uncovered(final long value)
    {
        buffer.putLong(offset + 36, value, BYTE_ORDER);
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

        final SettleFaultDecoder decoder = new SettleFaultDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
