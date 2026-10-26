/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * BROADCAST control on every gate/breaker change; ORIGIN on RegisterSession (the gateway's /ready input).
 */
@SuppressWarnings("all")
public final class AdmissionStateEncoder
{
    public static final int BLOCK_LENGTH = 27;
    public static final int TEMPLATE_ID = 109;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final AdmissionStateEncoder parentMessage = this;
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

    public AdmissionStateEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public AdmissionStateEncoder wrapAndApplyHeader(
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

    public AdmissionStateEncoder marketId(final int value)
    {
        buffer.putInt(offset + 0, value, BYTE_ORDER);
        return this;
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

    public AdmissionStateEncoder gateState(final GateState value)
    {
        buffer.putByte(offset + 4, (byte)value.value());
        return this;
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

    public AdmissionStateEncoder gateReason(final GateReason value)
    {
        buffer.putByte(offset + 5, (byte)value.value());
        return this;
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

    public AdmissionStateEncoder circuitBreaker(final BoolFlag value)
    {
        buffer.putByte(offset + 6, (byte)value.value());
        return this;
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

    public AdmissionStateEncoder atMs(final long value)
    {
        buffer.putLong(offset + 7, value, BYTE_ORDER);
        return this;
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

    public AdmissionStateEncoder logPosition(final long value)
    {
        buffer.putLong(offset + 15, value, BYTE_ORDER);
        return this;
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

    private final GateReasonsEncoder gateReasons = new GateReasonsEncoder();

    public GateReasonsEncoder gateReasons()
    {
        gateReasons.wrap(buffer, offset + 23);
        return gateReasons;
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

        final AdmissionStateDecoder decoder = new AdmissionStateDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
