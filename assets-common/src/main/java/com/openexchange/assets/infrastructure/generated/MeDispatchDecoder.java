/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * BROADCAST dispatch (forwarder sessions) / ORIGIN when answering RequestPendingDispatch. The forwarder encodes the ME DurableOrderCommand from this one-to-one and keeps no state. kind RELEASE_ID carries only the command id.
 */
@SuppressWarnings("all")
public final class MeDispatchDecoder
{
    public static final int BLOCK_LENGTH = 80;
    public static final int TEMPLATE_ID = 104;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final MeDispatchDecoder parentMessage = this;
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

    public MeDispatchDecoder wrap(
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

    public MeDispatchDecoder wrapAndApplyHeader(
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

    public MeDispatchDecoder sbeRewind()
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

    public long dispatchSeq()
    {
        return buffer.getLong(offset + 0, BYTE_ORDER);
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

    public long commandIdHigh()
    {
        return buffer.getLong(offset + 8, BYTE_ORDER);
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

    public long commandIdLow()
    {
        return buffer.getLong(offset + 16, BYTE_ORDER);
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

    public short kindRaw()
    {
        return ((short)(buffer.getByte(offset + 24) & 0xFF));
    }

    public DispatchKind kind()
    {
        return DispatchKind.get(((short)(buffer.getByte(offset + 24) & 0xFF)));
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

    public long userId()
    {
        return buffer.getLong(offset + 25, BYTE_ORDER);
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

    public long omsOrderId()
    {
        return buffer.getLong(offset + 33, BYTE_ORDER);
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

    public long oldOrderId()
    {
        return buffer.getLong(offset + 41, BYTE_ORDER);
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

    public int marketId()
    {
        return buffer.getInt(offset + 49, BYTE_ORDER);
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

    public short orderType()
    {
        return ((short)(buffer.getByte(offset + 53) & 0xFF));
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

    public short side()
    {
        return ((short)(buffer.getByte(offset + 54) & 0xFF));
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

    public long price()
    {
        return buffer.getLong(offset + 55, BYTE_ORDER);
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

    public long quantity()
    {
        return buffer.getLong(offset + 63, BYTE_ORDER);
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

    public long budget()
    {
        return buffer.getLong(offset + 71, BYTE_ORDER);
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

    public short timeInForce()
    {
        return ((short)(buffer.getByte(offset + 79) & 0xFF));
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final MeDispatchDecoder decoder = new MeDispatchDecoder();
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
        builder.append("[MeDispatch](sbeTemplateId=");
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
        builder.append("dispatchSeq=");
        builder.append(this.dispatchSeq());
        builder.append('|');
        builder.append("commandIdHigh=");
        builder.append(this.commandIdHigh());
        builder.append('|');
        builder.append("commandIdLow=");
        builder.append(this.commandIdLow());
        builder.append('|');
        builder.append("kind=");
        builder.append(this.kind());
        builder.append('|');
        builder.append("userId=");
        builder.append(this.userId());
        builder.append('|');
        builder.append("omsOrderId=");
        builder.append(this.omsOrderId());
        builder.append('|');
        builder.append("oldOrderId=");
        builder.append(this.oldOrderId());
        builder.append('|');
        builder.append("marketId=");
        builder.append(this.marketId());
        builder.append('|');
        builder.append("orderType=");
        builder.append(this.orderType());
        builder.append('|');
        builder.append("side=");
        builder.append(this.side());
        builder.append('|');
        builder.append("price=");
        builder.append(this.price());
        builder.append('|');
        builder.append("quantity=");
        builder.append(this.quantity());
        builder.append('|');
        builder.append("budget=");
        builder.append(this.budget());
        builder.append('|');
        builder.append("timeInForce=");
        builder.append(this.timeInForce());

        limit(originalLimit);

        return builder;
    }
    
    public MeDispatchDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
