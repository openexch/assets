/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * BROADCAST control on every gate/breaker change; ORIGIN on RegisterSession (the gateway's /ready input).
 */
@SuppressWarnings("all")
public final class AdmissionStateDecoder
{
    public static final int BLOCK_LENGTH = 27;
    public static final int TEMPLATE_ID = 109;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final AdmissionStateDecoder parentMessage = this;
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

    public AdmissionStateDecoder wrap(
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

    public AdmissionStateDecoder wrapAndApplyHeader(
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

    public AdmissionStateDecoder sbeRewind()
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

    public static int marketIdId()
    {
        return 1;
    }

    public static int marketIdSinceVersion()
    {
        return 0;
    }

    public static int marketIdEncodingOffset()
    {
        return 0;
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
        return buffer.getInt(offset + 0, BYTE_ORDER);
    }


    public static int gateStateId()
    {
        return 2;
    }

    public static int gateStateSinceVersion()
    {
        return 0;
    }

    public static int gateStateEncodingOffset()
    {
        return 4;
    }

    public static int gateStateEncodingLength()
    {
        return 1;
    }

    public static String gateStateMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short gateStateRaw()
    {
        return ((short)(buffer.getByte(offset + 4) & 0xFF));
    }

    public GateState gateState()
    {
        return GateState.get(((short)(buffer.getByte(offset + 4) & 0xFF)));
    }


    public static int gateReasonId()
    {
        return 3;
    }

    public static int gateReasonSinceVersion()
    {
        return 0;
    }

    public static int gateReasonEncodingOffset()
    {
        return 5;
    }

    public static int gateReasonEncodingLength()
    {
        return 1;
    }

    public static String gateReasonMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short gateReasonRaw()
    {
        return ((short)(buffer.getByte(offset + 5) & 0xFF));
    }

    public GateReason gateReason()
    {
        return GateReason.get(((short)(buffer.getByte(offset + 5) & 0xFF)));
    }


    public static int circuitBreakerId()
    {
        return 4;
    }

    public static int circuitBreakerSinceVersion()
    {
        return 0;
    }

    public static int circuitBreakerEncodingOffset()
    {
        return 6;
    }

    public static int circuitBreakerEncodingLength()
    {
        return 1;
    }

    public static String circuitBreakerMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short circuitBreakerRaw()
    {
        return ((short)(buffer.getByte(offset + 6) & 0xFF));
    }

    public BoolFlag circuitBreaker()
    {
        return BoolFlag.get(((short)(buffer.getByte(offset + 6) & 0xFF)));
    }


    public static int atMsId()
    {
        return 5;
    }

    public static int atMsSinceVersion()
    {
        return 0;
    }

    public static int atMsEncodingOffset()
    {
        return 7;
    }

    public static int atMsEncodingLength()
    {
        return 8;
    }

    public static String atMsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long atMsNullValue()
    {
        return -9223372036854775808L;
    }

    public static long atMsMinValue()
    {
        return -9223372036854775807L;
    }

    public static long atMsMaxValue()
    {
        return 9223372036854775807L;
    }

    public long atMs()
    {
        return buffer.getLong(offset + 7, BYTE_ORDER);
    }


    public static int logPositionId()
    {
        return 6;
    }

    public static int logPositionSinceVersion()
    {
        return 0;
    }

    public static int logPositionEncodingOffset()
    {
        return 15;
    }

    public static int logPositionEncodingLength()
    {
        return 8;
    }

    public static String logPositionMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long logPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long logPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long logPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public long logPosition()
    {
        return buffer.getLong(offset + 15, BYTE_ORDER);
    }


    public static int gateReasonsId()
    {
        return 7;
    }

    public static int gateReasonsSinceVersion()
    {
        return 7;
    }

    public static int gateReasonsEncodingOffset()
    {
        return 23;
    }

    public static int gateReasonsEncodingLength()
    {
        return 4;
    }

    public static String gateReasonsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    private final GateReasonsDecoder gateReasons = new GateReasonsDecoder();

    public GateReasonsDecoder gateReasons()
    {
        if (parentMessage.actingVersion < 7)
        {
            return null;
        }

        gateReasons.wrap(buffer, offset + 23);
        return gateReasons;
    }

    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final AdmissionStateDecoder decoder = new AdmissionStateDecoder();
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
        builder.append("[AdmissionState](sbeTemplateId=");
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
        builder.append("marketId=");
        builder.append(this.marketId());
        builder.append('|');
        builder.append("gateState=");
        builder.append(this.gateState());
        builder.append('|');
        builder.append("gateReason=");
        builder.append(this.gateReason());
        builder.append('|');
        builder.append("circuitBreaker=");
        builder.append(this.circuitBreaker());
        builder.append('|');
        builder.append("atMs=");
        builder.append(this.atMs());
        builder.append('|');
        builder.append("logPosition=");
        builder.append(this.logPosition());
        builder.append('|');
        builder.append("gateReasons=");
        final GateReasonsDecoder gateReasons = this.gateReasons();
        if (null != gateReasons)
        {
            gateReasons.appendTo(builder);
        }
        else
        {
            builder.append("null");
        }

        limit(originalLimit);

        return builder;
    }
    
    public AdmissionStateDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
