using System;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Buffers;
using Microsoft.CodeAnalysis.Formatting;

class XDREventMessage
{
    public string Sourcetype { get; set; }
    public double Time { get; set; }
    public DenormalizedRecord @event { get; set; }

    public XDREventMessage (DenormalizedRecord xdrEvent)
    {
        Sourcetype = "amdl:nsg:flowlogs";
        Time = unixTime(xdrEvent.Time);
        @event = xdrEvent;
    }

    double unixTime(string time)
    {
        DateTime t = DateTime.ParseExact(time,"yyyy-MM-ddTHH:mm:ss.fffffffZ", System.Globalization.CultureInfo.InvariantCulture);

        double unixTimestamp = t.Ticks - new DateTime(1970, 1, 1).Ticks;
        unixTimestamp /= TimeSpan.TicksPerSecond;
        return unixTimestamp;
    }

    public int GetSizeOfObject()
    {
        return Sourcetype.Length + 10 + 6 + (@event == null ? 0 : @event.GetSizeOfJSONObject());
    }
}

class DenormalizedRecord
{
    public string Time { get; set; }
    public string Category { get; set; }
    public string OperationName { get; set; }
    public string ResourceId { get; set; }
    public float Version { get; set; }
    public string? DeviceExtId { get; set; }
    public string NsgRuleName { get; set; }
    public string Mac { get; set; }
    public string StartTime { get; set; }
    public string SourceAddress { get; set; }
    public string DestinationAddress { get; set; }
    public string SourcePort { get; set; }
    public string DestinationPort { get; set; }
    public string TransportProtocol { get; set; }
    public string DeviceDirection { get; set; }
    public string DeviceAction { get; set; }
    public string? FlowState { get; set; }
    public string? PacketsStoD { get; set; }
    public string? BytesStoD { get; set; }
    public string? PacketsDtoS { get; set; }
    public string? BytesDtoS { get; set; }

    public DenormalizedRecord(
        float version,
        string time,
        string category,
        string operationName,
        string resourceId,
        string nsgRuleName,
        string mac,
        NSGFlowLogTuple tuple
        )
    {
        Version = version;
        Time = time;
        Category = category;
        OperationName = operationName;
        ResourceId = resourceId;
        NsgRuleName = nsgRuleName;
        Mac = mac;
        StartTime = tuple.StartTime;
        SourceAddress = tuple.SourceAddress;
        DestinationAddress = tuple.DestinationAddress;
        SourcePort = tuple.SourcePort;
        DestinationPort = tuple.DestinationPort;
        TransportProtocol = tuple.TransportProtocol;
        DeviceDirection = tuple.DeviceDirection;
        DeviceAction = tuple.DeviceAction;
        if (Version >= 2.0)
        {
            FlowState = tuple.FlowState;
            PacketsDtoS = tuple.PacketsDtoS;
            PacketsStoD = tuple.PacketsStoD;
            BytesDtoS = tuple.BytesDtoS;
            BytesStoD = tuple.BytesStoD;
        }
    }

    private string MakeMAC()
    {
        StringBuilder sb = StringBuilderPool.Allocate();
        string delimitedMac = "";
        try
        {
            sb.Append(Mac.AsSpan(0, 2)).Append(':');
            sb.Append(Mac.AsSpan(2, 2)).Append(':');
            sb.Append(Mac.AsSpan(4, 2)).Append(':');
            sb.Append(Mac.AsSpan(6, 2)).Append(':');
            sb.Append(Mac.AsSpan(8, 2)).Append(':');
            sb.Append(Mac.AsSpan(10, 2));

            delimitedMac = sb.ToString();
        }
        finally
        {
            StringBuilderPool.Free(sb);
        }

        return delimitedMac;
    }

    private string MakeDeviceExternalID()
    {
        var patternSubscriptionId = "SUBSCRIPTIONS\\/(.*?)\\/";
        var patternResourceGroup = "SUBSCRIPTIONS\\/(?:.*?)\\/RESOURCEGROUPS\\/(.*?)\\/";
        var patternResourceName = "PROVIDERS\\/(?:.*?\\/.*?\\/)(.*?)(?:\\/|$)";

        Match m = Regex.Match(ResourceId, patternSubscriptionId);
        var subscriptionID = m.Groups[1].Value;

        m = Regex.Match(ResourceId, patternResourceGroup);
        var resourceGroup = m.Groups[1].Value;

        m = Regex.Match(ResourceId, patternResourceName);
        var resourceName = m.Groups[1].Value;

        return subscriptionID + "/" + resourceGroup + "/" + resourceName;
    }

    private string MakeCEFTime()
    {
        // sample input: "2017-08-09T00:13:25.4850000Z"
        // sample output: Aug 09 00:13:25 host CEF:0

        CultureInfo culture = new CultureInfo("en-US");
        DateTime tempDate = Convert.ToDateTime(Time, culture);
        string newTime = tempDate.ToString("MMM dd HH:mm:ss");

        return newTime + " host CEF:0";
    }

    private void BuildCEF(ref StringBuilder sb)
    {
        sb.Append(MakeCEFTime());
        sb.Append("|Microsoft.Network");
        sb.Append("|NETWORKSECURITYGROUPS");
        sb.Append('|').Append(Version.ToString("0.0"));
        sb.Append('|').Append(Category);
        sb.Append('|').Append(OperationName);
        sb.Append("|1");  // severity is always 1
        sb.Append("|deviceExternalId=").Append(MakeDeviceExternalID());

        sb.Append(string.Format(" cs1={0}", NsgRuleName));
        sb.Append(string.Format(" cs1Label=NSGRuleName"));

        sb.Append((DeviceDirection == "I" ? " dmac=" : " smac=") + MakeMAC());

        sb.Append(" rt=").Append((Convert.ToUInt64(StartTime) * 1000).ToString());
        sb.Append(" src=").Append(SourceAddress);
        sb.Append(" dst=").Append(DestinationAddress);
        sb.Append(" spt=").Append(SourcePort);
        sb.Append(" dpt=").Append(DestinationPort);
        sb.Append(" proto=").Append(TransportProtocol == "U" ? "UDP" : "TCP");
        sb.Append(" deviceDirection=").Append(DeviceDirection == "I" ? "0" : "1");
        sb.Append(" act=").Append(DeviceAction);

        if (Version >= 2.0)
        {
            // add fields from version 2 schema
            sb.Append(" cs2=").Append(FlowState);
            sb.Append(" cs2Label=FlowState");

            if (FlowState != "B")
            {
                sb.Append(" cn1=").Append(PacketsStoD);
                sb.Append(" cn1Label=PacketsStoD");
                sb.Append(" cn2=").Append(PacketsDtoS);
                sb.Append(" cn2Label=PacketsDtoS");

                if (DeviceDirection == "I")
                {
                    sb.Append(" bytesIn=").Append(BytesStoD);
                    sb.Append(" bytesOut=").Append(BytesDtoS);
                }
                else
                {
                    sb.Append(" bytesIn=").Append(BytesDtoS);
                    sb.Append(" bytesOut=").Append(BytesStoD);
                }
            }
        }
    }

    public int AppendToTransmission(ref byte[] transmission, int maxSize, int offset)
    {
        StringBuilder sb = StringBuilderPool.Allocate();
        var bytePool = ArrayPool<byte>.Shared;
        byte[] buffer = bytePool.Rent(1000);
        byte[] crlf = "\r\n"u8.ToArray();
        int bytesToAppend = 0;

        try
        {
            BuildCEF(ref sb);

            string s = sb.ToString();
            bytesToAppend += s.Length + 2;

            if (maxSize > offset + bytesToAppend)
            {
                Buffer.BlockCopy(Encoding.ASCII.GetBytes(s), 0, buffer, 0, s.Length);
                Buffer.BlockCopy(crlf, 0, buffer, s.Length, 2);

                Buffer.BlockCopy(buffer, 0, transmission, offset, bytesToAppend);
            } else
            {
                throw new System.IO.InternalBufferOverflowException("ArcSight transmission buffer overflow");
            }

        }
        finally
        {
            StringBuilderPool.Free(sb);
            bytePool.Return(buffer);
        }

        return bytesToAppend;
    }

    public int GetSizeOfJSONObject()
    {
        int objectSize = 0;

        objectSize += Version.ToString().Length + 7 + 6;
        objectSize += Time.Length + 4 + 6;
        objectSize += Category.Length + 8 + 6;
        objectSize += OperationName.Length + 13 + 6;
        objectSize += ResourceId.Length + 10 + 6;
        objectSize += NsgRuleName.Length + 11 + 6;
        objectSize += Mac.Length + 3 + 6;
        objectSize += StartTime.Length + 9 + 6;
        objectSize += SourceAddress.Length + 13 + 6;
        objectSize += DestinationAddress.Length + 18 + 6;
        objectSize += SourcePort.Length + 10 + 6;
        objectSize += DestinationPort.Length + 15 + 6;
        objectSize += DeviceDirection.Length + 15 + 6;
        objectSize += DeviceAction.Length + 12 + 6;
        objectSize += TransportProtocol.Length + 17 + 6;
        if (Version >= 2.0)
        {
            objectSize += FlowState.Length + 9 + 6;
            objectSize += PacketsDtoS == null ? 0 : PacketsDtoS.Length + 11 + 6;
            objectSize += PacketsStoD == null ? 0 : PacketsStoD.Length + 11 + 6;
            objectSize += BytesDtoS == null ? 0 : BytesDtoS.Length + 9 + 6;
            objectSize += BytesStoD == null ? 0 : BytesStoD.Length + 9 + 6;
        }
        return objectSize;
    }
}

class OutgoingRecords
{
    public List<DenormalizedRecord> Records { get; set; }
}

class NSGFlowLogTuple
{
    public float schemaVersion { get; set; }

    public string StartTime { get; set; }
    public string SourceAddress { get; set; }
    public string DestinationAddress { get; set; }
    public string SourcePort { get; set; }
    public string DestinationPort { get; set; }
    public string TransportProtocol { get; set; }
    public string DeviceDirection { get; set; }
    public string DeviceAction { get; set; }

    // version 2 tuple properties
    public string FlowState { get; set; }
    public string PacketsStoD { get; set; }
    public string BytesStoD { get; set; }
    public string PacketsDtoS { get; set; }
    public string BytesDtoS { get; set; }

    public NSGFlowLogTuple(string tuple, float version)
    {
        schemaVersion = version;

        char[] sep = [','];
        string[] parts = tuple.Split(sep);
        StartTime = parts[0];
        SourceAddress = parts[1];
        DestinationAddress = parts[2];
        SourcePort = parts[3];
        DestinationPort = parts[4];
        TransportProtocol = parts[5];
        DeviceDirection = parts[6];
        DeviceAction = parts[7];

        if (version >= 2.0)
        {
            FlowState = parts[8];
            if (FlowState != "B")
            {
                PacketsStoD = parts[9] == "" ? "0" : parts[9];
                BytesStoD = parts[10] == "" ? "0" : parts[10];
                PacketsDtoS = parts[11] == "" ? "0" : parts[11];
                BytesDtoS = parts[12] == "" ? "0" : parts[12];
            }
        }
    }

    public string GetDirection
    {
        get { return DeviceDirection; }
    }

    public override string ToString()
    {
        var temp = new StringBuilder();
        temp.Append("rt=").Append((Convert.ToUInt64(StartTime) * 1000).ToString());
        temp.Append(" src=").Append(SourceAddress);
        temp.Append(" dst=").Append(DestinationAddress);
        temp.Append(" spt=").Append(SourcePort);
        temp.Append(" dpt=").Append(DestinationPort);
        temp.Append(" proto=").Append(TransportProtocol == "U" ? "UDP" : "TCP");
        temp.Append(" deviceDirection=").Append(DeviceDirection == "I" ? "0" : "1");
        temp.Append(" act=").Append(DeviceAction);

        if (schemaVersion >= 2.0)
        {
            // add fields from version 2 schema
            temp.Append(" cs2=").Append(FlowState);
            temp.Append(" cs2Label=FlowState");

            if (FlowState != "B")
            {
                temp.Append(" cn1=").Append(PacketsStoD);
                temp.Append(" cn1Label=PacketsStoD");
                temp.Append(" cn2=").Append(PacketsDtoS);
                temp.Append(" cn2Label=PacketsDtoS");

                if (DeviceDirection == "I")
                {
                    temp.Append(" bytesIn=").Append(BytesStoD);
                    temp.Append(" bytesOut=").Append(BytesDtoS);
                }
                else
                {
                    temp.Append(" bytesIn=").Append(BytesDtoS);
                    temp.Append(" bytesOut=").Append(BytesStoD);
                }
            }
        }

        return temp.ToString();
    }

    public string JsonSubString()
    {
        var sb = new StringBuilder();
        sb.Append(",\"rt\":\"").Append((Convert.ToUInt64(StartTime) * 1000).ToString()).Append('"');
        sb.Append(",\"src\":\"").Append(SourceAddress).Append('"');
        sb.Append(",\"dst\":\"").Append(DestinationAddress).Append('"');
        sb.Append(",\"spt\":\"").Append(SourcePort).Append('"');
        sb.Append(",\"dpt\":\"").Append(DestinationPort).Append('"');
        sb.Append(",\"proto\":\"").Append(TransportProtocol == "U" ? "UDP" : "TCP").Append('"');
        sb.Append(",\"deviceDirection\":\"").Append(DeviceDirection == "I" ? "0" : "1").Append('"');
        sb.Append(",\"act\":\"").Append(DeviceAction).Append('"');

        return sb.ToString();
    }
}

class NSGFlowLogsInnerFlows
{
    public string Mac { get; set; }
    public string[] FlowTuples { get; set; }

    public string MakeMAC()
    {
        var temp = new StringBuilder();
        temp.Append(Mac.AsSpan(0, 2)).Append(':');
        temp.Append(Mac.AsSpan(2, 2)).Append(':');
        temp.Append(Mac.AsSpan(4, 2)).Append(':');
        temp.Append(Mac.AsSpan(6, 2)).Append(':');
        temp.Append(Mac.AsSpan(8, 2)).Append(':');
        temp.Append(Mac.AsSpan(10, 2));

        return temp.ToString();
    }
}

class NSGFlowLogsOuterFlows
{
    public string Rule { get; set; }
    public NSGFlowLogsInnerFlows[] Flows { get; set; }
}

class NSGFlowLogProperties
{
    public float Version { get; set; }
    public NSGFlowLogsOuterFlows[] Flows { get; set; }
}

class NSGFlowLogRecord
{
    public string Time { get; set; }
    public string SystemId { get; set; }
    public string MacAddress { get; set; }
    public string Category { get; set; }
    public string ResourceId { get; set; }
    public string OperationName { get; set; }
    public NSGFlowLogProperties Properties { get; set; }

    public string MakeDeviceExternalID()
    {
        var patternSubscriptionId = "SUBSCRIPTIONS\\/(.*?)\\/";
        var patternResourceGroup = "SUBSCRIPTIONS\\/(?:.*?)\\/RESOURCEGROUPS\\/(.*?)\\/";
        var patternResourceName = "PROVIDERS\\/(?:.*?\\/.*?\\/)(.*?)(?:\\/|$)";

        Match m = Regex.Match(ResourceId, patternSubscriptionId);
        var subscriptionID = m.Groups[1].Value;

        m = Regex.Match(ResourceId, patternResourceGroup);
        var resourceGroup = m.Groups[1].Value;

        m = Regex.Match(ResourceId, patternResourceName);
        var resourceName = m.Groups[1].Value;

        return subscriptionID + "/" + resourceGroup + "/" + resourceName;
    }

    public string MakeCEFTime()
    {
        // sample input: "2017-08-09T00:13:25.4850000Z"
        // sample output: Aug 09 00:13:25 host CEF:0

        CultureInfo culture = new CultureInfo("en-US");
        DateTime tempDate = Convert.ToDateTime(Time, culture);
        string newTime = tempDate.ToString("MMM dd HH:mm:ss");

        return newTime + " host CEF:0";
    }

    public override string ToString()
    {
        string temp = MakeDeviceExternalID();
        return temp;
    }
}

class NSGFlowLogRecords
{
    public NSGFlowLogRecord[] Records { get; set; }
}
