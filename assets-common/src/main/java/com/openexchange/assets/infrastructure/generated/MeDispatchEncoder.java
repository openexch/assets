/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * BROADCAST dispatch (forwarder sessions) / ORIGIN when answering RequestPendingDispatch. The forwarder encodes the ME DurableOrderCommand from this one-to-one and keeps no state. kind RELEASE_ID carries only the command id.
 */
@SuppressWarnings("all")
public final class MeDispatchEncoder
{
    public static final int BLOCK_LENGTH = 80;
    public static final int TEMPLATE_ID = 104;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final MeDispatchEncoder parentMessage = this;
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

    public MeDispatchEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public MeDispatchEncoder wrapAndApplyHeader(
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

    public static int dispatchSeqId()
    {
        return 1;
    }

    public static int dispatchSeqSinceVersion()
    {
        return 0;
    }

    public static int dispatchSeqEncodingOffset()
    {
        return 0;
    }

    public static int dispatchSeqEncodingLength()
    {
        return 8;
    }

    public static String dispatchSeqMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long dispatchSeqNullValue()
    {
        return -9223372036854775808L;
    }

    public static long dispatchSeqMinValue()
    {
        return -9223372036854775807L;
    }

    public static long dispatchSeqMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeDispatchEncoder dispatchSeq(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
    }


    public static int commandIdHighId()
    {
        return 2;
    }

    public static int commandIdHighSinceVersion()
    {
        return 0;
    }

    public static int commandIdHighEncodingOffset()
    {
        return 8;
    }

    public static int commandIdHighEncodingLength()
    {
        return 8;
    }

    public static String commandIdHighMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long commandIdHighNullValue()
    {
        return -9223372036854775808L;
    }

    public static long commandIdHighMinValue()
    {
        return -9223372036854775807L;
    }

    public static long commandIdHighMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeDispatchEncoder commandIdHigh(final long value)
    {
        buffer.putLong(offset + 8, value, BYTE_ORDER);
        return this;
    }


    public static int commandIdLowId()
    {
        return 3;
    }

    public static int commandIdLowSinceVersion()
    {
        return 0;
    }

    public static int commandIdLowEncodingOffset()
    {
        return 16;
    }

    public static int commandIdLowEncodingLength()
    {
        return 8;
    }

    public static String commandIdLowMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long commandIdLowNullValue()
    {
        return -9223372036854775808L;
    }

    public static long commandIdLowMinValue()
    {
        return -9223372036854775807L;
    }

    public static long commandIdLowMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeDispatchEncoder commandIdLow(final long value)
    {
        buffer.putLong(offset + 16, value, BYTE_ORDER);
        return this;
    }


    public static int kindId()
    {
        return 4;
    }

    public static int kindSinceVersion()
    {
        return 0;
    }

    public static int kindEncodingOffset()
    {
        return 24;
    }

    public static int kindEncodingLength()
    {
        return 1;
    }

    public static String kindMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public MeDispatchEncoder kind(final DispatchKind value)
    {
        buffer.putByte(offset + 24, (byte)value.value());
        return this;
    }

    public static int userIdId()
    {
        return 5;
    }

    public static int userIdSinceVersion()
    {
        return 0;
    }

    public static int userIdEncodingOffset()
    {
        return 25;
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

    public MeDispatchEncoder userId(final long value)
    {
        buffer.putLong(offset + 25, value, BYTE_ORDER);
        return this;
    }


    public static int omsOrderIdId()
    {
        return 6;
    }

    public static int omsOrderIdSinceVersion()
    {
        return 0;
    }

    public static int omsOrderIdEncodingOffset()
    {
        return 33;
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

    public MeDispatchEncoder omsOrderId(final long value)
    {
        buffer.putLong(offset + 33, value, BYTE_ORDER);
        return this;
    }


    public static int oldOrderIdId()
    {
        return 7;
    }

    public static int oldOrderIdSinceVersion()
    {
        return 0;
    }

    public static int oldOrderIdEncodingOffset()
    {
        return 41;
    }

    public static int oldOrderIdEncodingLength()
    {
        return 8;
    }

    public static String oldOrderIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long oldOrderIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long oldOrderIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long oldOrderIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeDispatchEncoder oldOrderId(final long value)
    {
        buffer.putLong(offset + 41, value, BYTE_ORDER);
        return this;
    }


    public static int marketIdId()
    {
        return 8;
    }

    public static int marketIdSinceVersion()
    {
        return 0;
    }

    public static int marketIdEncodingOffset()
    {
        return 49;
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

    public MeDispatchEncoder marketId(final int value)
    {
        buffer.putInt(offset + 49, value, BYTE_ORDER);
        return this;
    }


    public static int orderTypeId()
    {
        return 9;
    }

    public static int orderTypeSinceVersion()
    {
        return 0;
    }

    public static int orderTypeEncodingOffset()
    {
        return 53;
    }

    public static int orderTypeEncodingLength()
    {
        return 1;
    }

    public static String orderTypeMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static short orderTypeNullValue()
    {
        return (short)255;
    }

    public static short orderTypeMinValue()
    {
        return (short)0;
    }

    public static short orderTypeMaxValue()
    {
        return (short)254;
    }

    public MeDispatchEncoder orderType(final short value)
    {
        buffer.putByte(offset + 53, (byte)value);
        return this;
    }


    public static int sideId()
    {
        return 10;
    }

    public static int sideSinceVersion()
    {
        return 0;
    }

    public static int sideEncodingOffset()
    {
        return 54;
    }

    public static int sideEncodingLength()
    {
        return 1;
    }

    public static String sideMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static short sideNullValue()
    {
        return (short)255;
    }

    public static short sideMinValue()
    {
        return (short)0;
    }

    public static short sideMaxValue()
    {
        return (short)254;
    }

    public MeDispatchEncoder side(final short value)
    {
        buffer.putByte(offset + 54, (byte)value);
        return this;
    }


    public static int priceId()
    {
        return 11;
    }

    public static int priceSinceVersion()
    {
        return 0;
    }

    public static int priceEncodingOffset()
    {
        return 55;
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

    public MeDispatchEncoder price(final long value)
    {
        buffer.putLong(offset + 55, value, BYTE_ORDER);
        return this;
    }


    public static int quantityId()
    {
        return 12;
    }

    public static int quantitySinceVersion()
    {
        return 0;
    }

    public static int quantityEncodingOffset()
    {
        return 63;
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

    public MeDispatchEncoder quantity(final long value)
    {
        buffer.putLong(offset + 63, value, BYTE_ORDER);
        return this;
    }


    public static int budgetId()
    {
        return 13;
    }

    public static int budgetSinceVersion()
    {
        return 0;
    }

    public static int budgetEncodingOffset()
    {
        return 71;
    }

    public static int budgetEncodingLength()
    {
        return 8;
    }

    public static String budgetMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long budgetNullValue()
    {
        return -9223372036854775808L;
    }

    public static long budgetMinValue()
    {
        return -9223372036854775807L;
    }

    public static long budgetMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeDispatchEncoder budget(final long value)
    {
        buffer.putLong(offset + 71, value, BYTE_ORDER);
        return this;
    }


    public static int timeInForceId()
    {
        return 14;
    }

    public static int timeInForceSinceVersion()
    {
        return 0;
    }

    public static int timeInForceEncodingOffset()
    {
        return 79;
    }

    public static int timeInForceEncodingLength()
    {
        return 1;
    }

    public static String timeInForceMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static short timeInForceNullValue()
    {
        return (short)255;
    }

    public static short timeInForceMinValue()
    {
        return (short)0;
    }

    public static short timeInForceMaxValue()
    {
        return (short)254;
    }

    public MeDispatchEncoder timeInForce(final short value)
    {
        buffer.putByte(offset + 79, (byte)value);
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

        final MeDispatchDecoder decoder = new MeDispatchDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
