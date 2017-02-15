using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Collections.Generic;
using UnityEngine;

namespace MobaNet
{
    public class SendingPackage
    {
        public byte[] Content;
        public UInt32 SendingSequenceNo;
        public float LastSendTimestamp;
        public float FirstSendTimestamp;
        public float RetransmissionInterval;
        public int fastack = 0;
    }

    public class RecvingPackage 
    {
        public byte[] Data;
        public ushort MaxPiece;
        public UInt32 RecvingSequenceNo;
    }

    public class RUDP: IConnection
    {
        public static ushort MTU = 1400;
        public float DEFAULT_RTO = 0.1f;
        public float MAX_RTO = 2f;
        public float RTO;//in millisecond

        static readonly public int msgIdSize = 4;
        public bool reverseByte = true;

        public ushort MaxWaitingSendLength = 32;
        private Queue<SendingPackage> _sendQueue;//all input waiting here
        private Queue<byte[]> _sendBuffer;//data in stream
        private List<SendingPackage> _waitAckList;

        private List<RecvingPackage> _recvQueue;//data in stream
        private Queue<RecvingPackage> _recvBuffer;//wait to get
        private const int MaxRecvWindSize = 128;
        private UInt32 _una;

        private UInt32 _currentSnedSeq;

        private const ushort DataFrameHeaderLength = 72 / 8;
        private const ushort PublicFrameHeaderLength = 80 / 8;

        private uint SessionID;
        private float Clock;

        enum PACKAGE_CATE
        {
            DATA = 4,
            ACK = 3,
        }

        public RUDP()
        {
            _sendQueue = new Queue<SendingPackage>();
            _sendBuffer = new Queue<byte[]>();
            _waitAckList = new List<SendingPackage>();

            _recvQueue = new List<RecvingPackage>(MaxRecvWindSize);
            _recvBuffer = new Queue<RecvingPackage>();
            RUDPReset();
        }

        public void RUDPReset()
        {
            Debug.Log("RUDPReset");
            _sendQueue.Clear();
            _sendBuffer.Clear();
            _waitAckList.Clear();

            _recvQueue.Clear();
            for (int i = 0; i < MaxRecvWindSize; i++)
            {
                _recvQueue.Add(null);
            }
            _recvBuffer.Clear();
               
            _una = 0;
            _currentSnedSeq = 1;
            SessionID = 0;
            Close();
        }

        public bool RUDPConnect(IPEndPoint remoteIp, uint sessionID)
        {
            Connect(remoteIp);
            SessionID = sessionID;
            Clock = 0;
            return true;
        }

        public void Tick(float deltaTime)
        {
            Clock += deltaTime;
            byte[] rawData = null;
            int len = 0;
            while (Recv(ref rawData, ref len))
            {
                if (rawData != null && len > 0)
                {
                    ProcessRecvQueue(rawData, len);
                }
            }

            ProcessSendQueue(deltaTime);
        }

        byte[] SendStreamBuffer = new byte[MTU]; // for final frame construction 
        void ProcessSendQueue(float deltaTime)
        {
            //Put available packages to waiting dict
            int readyToSendNum = 0;

            readyToSendNum = Math.Min(MaxWaitingSendLength - _waitAckList.Count, _sendQueue.Count);
            for (int i = 0; i < readyToSendNum; i++)
            {
                SendingPackage package = _sendQueue.Dequeue();
                _sendBuffer.Enqueue(package.Content);
                package.LastSendTimestamp = Clock;
                package.FirstSendTimestamp = Clock;
                _waitAckList.Add(package);
            }

            //Re-send un-acked packages
            //string pendingList = "";
            for (int i = 0; i < _waitAckList.Count; i++)
            {
                SendingPackage package = _waitAckList[i];
                float elapsedTime = Clock - package.LastSendTimestamp;
                bool needRetransmit = elapsedTime > package.RetransmissionInterval;
                bool fastack = package.fastack >= 2;
                if (needRetransmit || fastack)
                {
                    _sendBuffer.Enqueue(package.Content);
                    package.LastSendTimestamp = Clock;
                    if(fastack)
                    {
                        package.fastack = 0;
                    }
                    else if(needRetransmit)
                    {
                        RTO = (uint)(RTO * 1.5 > MAX_RTO ? MAX_RTO : RTO * 1.5);
                        package.RetransmissionInterval = RTO;
                    }
                    
                }
            }

            int currentPos = PublicFrameHeaderLength;
            //actually send
            int iWaitAck = 0;
            while (_sendBuffer.Count > 0)
            {
                byte[] nextSendContent = _sendBuffer.Dequeue();
                if (currentPos + nextSendContent.Length > MTU)
                {
                    for (; iWaitAck < _waitAckList.Count; iWaitAck++)
                    {
                        if(Clock - _waitAckList[iWaitAck].LastSendTimestamp  < 0.001f)
                        {
                            continue;
                        }
                        nextSendContent = _waitAckList[iWaitAck].Content;
                        if (currentPos + nextSendContent.Length > MTU)
                        {
                            break;
                        }
                        Array.Copy(nextSendContent, 0, SendStreamBuffer, currentPos, nextSendContent.Length);
                        currentPos += nextSendContent.Length;
                    }

                    Send(SendStreamBuffer, currentPos);
                    currentPos = PublicFrameHeaderLength;
                }
                Array.Copy(nextSendContent, 0, SendStreamBuffer, currentPos, nextSendContent.Length);
                currentPos += nextSendContent.Length;
            }
            if(currentPos > PublicFrameHeaderLength)
            {
                for(; iWaitAck < _waitAckList.Count; iWaitAck++)
                {
                    if (Clock - _waitAckList[iWaitAck].LastSendTimestamp < 0.001f)
                    {
                        continue;
                    }
                    byte[] nextSendContent = _waitAckList[iWaitAck].Content;
                    if (currentPos + nextSendContent.Length > MTU)
                    {
                        break;
                    }
                    Array.Copy(nextSendContent, 0, SendStreamBuffer, currentPos, nextSendContent.Length);
                    currentPos += nextSendContent.Length;
                }
                
                Send(SendStreamBuffer, currentPos);
            }
        }

        //DateTime lastTime;
        void ProcessRecvQueue(byte[] rawData, int len)
        {    
            MemoryStream msgStream = new MemoryStream(rawData);
            BinaryReader reader = new BinaryReader(msgStream);
            //CRC
            byte[] checksumBytes = reader.ReadBytes(2);

            //SESSION
            byte[] sessionBytes = reader.ReadBytes(4);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(sessionBytes);
            UInt32 sessionID = BitConverter.ToUInt32(sessionBytes, 0);
            //Discard mismatch session package
            if(sessionID != this.SessionID)
            {
                return;
            }

            //UNA
            byte[] unaBytes = reader.ReadBytes(4);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(unaBytes);
            UInt32 coUna = BitConverter.ToUInt32(unaBytes, 0);

            len -= PublicFrameHeaderLength;

            uint maxAckSeq = 0;

            while(len > 0)
            {
                //len
                byte[] lenBytes = reader.ReadBytes(2);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(lenBytes);
                ushort currentLen = BitConverter.ToUInt16(lenBytes, 0);
                len -= (currentLen + 2);

                //Get controll bits
                byte control = reader.ReadByte();
                if (control == (byte)PACKAGE_CATE.DATA)
                {
                    //this is a data frame
                    byte[] seqDataBytes = reader.ReadBytes(4);
                    if (BitConverter.IsLittleEndian)
                        Array.Reverse(seqDataBytes);
                    UInt32 seqData = BitConverter.ToUInt32(seqDataBytes, 0);
                    byte[] maxPieceBytes = reader.ReadBytes(2);
                    if (BitConverter.IsLittleEndian)
                        Array.Reverse(maxPieceBytes);
                    ushort maxPiece = BitConverter.ToUInt16(maxPieceBytes, 0);
                    byte[] data = reader.ReadBytes(currentLen - DataFrameHeaderLength + 2);
                    if (seqData > _una && seqData < _una + MaxRecvWindSize)
                    {
                        int recvQueuePos = (int)(seqData - _una - 1);
                        if (_recvQueue[recvQueuePos] == null)
                        {
                            //replace dummy packages
                            RecvingPackage recvPackage = new RecvingPackage();
                            recvPackage.Data = data;
                            recvPackage.MaxPiece = maxPiece;
                            recvPackage.RecvingSequenceNo = seqData;
                            _recvQueue[recvQueuePos] = recvPackage;

                            //Calculate una
                            int i = 0;
                            for(; i < _recvQueue.Count; i++, _una++)
                            {
                                if (_recvQueue[i] == null)
                                {
                                    break;
                                }
                                else
                                {
                                    _recvBuffer.Enqueue(_recvQueue[i]);
                                    _recvQueue.Add(null);
                                }
                            }
                            _recvQueue.RemoveRange(0, i);
                        }
                        SendAck(seqData);
                    }                  
                      
                }
                else if (control == (byte)PACKAGE_CATE.ACK) //ACK, FIN+ACK
                {
                    byte[] seqDataBytes = reader.ReadBytes(4);
                    if (BitConverter.IsLittleEndian)
                        Array.Reverse(seqDataBytes);
                    UInt32 seqData = BitConverter.ToUInt32(seqDataBytes, 0);
                    //Debug.Log("Recv Ack SeqNo: " + seqData.ToString());

                    SendingPackage sendPackage = _waitAckList.Find((SendingPackage input) => input.SendingSequenceNo == seqData);
                    if (sendPackage != null)
                    {
                        int newPing = (int)(Clock - sendPackage.FirstSendTimestamp);
                        MobaNetworkManager.Instance.AddPing(newPing);
                        _waitAckList.Remove(sendPackage);

                        if(maxAckSeq < seqData)
                        {
                            maxAckSeq = seqData;
                        }
                    }

                    //Get an ack, set all rto to default
                    RTO = DEFAULT_RTO;
                    for(int i = 0; i < _waitAckList.Count; i++)
                    {
                        _waitAckList[i].RetransmissionInterval = RTO;
                    }
                    
                }
                else
                {
                    Debug.LogError("Receive Illegal Package");
                }
            }

            //fastack
            for (int i = 0; i < _waitAckList.Count; i++)
            {
                if (_waitAckList[i].SendingSequenceNo < maxAckSeq)
                {
                    _waitAckList[i].fastack++;
                }
            }

            //process correspondance's una
            ProcessCoUna(coUna);
        }

        void ProcessCoUna(uint coUna)
        {
            for (int i = _waitAckList.Count - 1; i >= 0; i--)
            {
                if (_waitAckList[i].SendingSequenceNo <= coUna)
                {
                    _waitAckList.RemoveAt(i);
                }
            }
        }

        void SendAck(UInt32 seqNo)
        {
            if(seqNo < _una)
            {
                return;
            }
            byte[] ackFrame = new byte[7];
            MemoryStream ms = new MemoryStream(ackFrame);
            BinaryWriter bw = new BinaryWriter(ms);

            byte[] lenBytes = BitConverter.GetBytes((ushort)5);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(lenBytes);
            bw.Write(lenBytes);//0 len
            bw.Write((byte)3); // control 2
            byte[] seqBytes = BitConverter.GetBytes(seqNo);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(seqBytes);
            bw.Write(seqBytes); //seqNo 3

            bw.Close();
            ms.Flush();

            _sendBuffer.Enqueue(ackFrame);
        }
#region IsConnected
        public bool IsConnected()
        {
            return SessionID != 0;
        }
#endregion

#region GetMsg
        public MsgObject GetMsg()
        {
            byte[] thepacket = GetReliableMsg();
            if (thepacket == null)
                return null;
            return ProcessData(thepacket);
        }

        MsgObject ProcessData(byte[] thepacket)
        {
            MemoryStream msgStream = new MemoryStream(thepacket, 0, thepacket.Length);
            BinaryReader reader = new BinaryReader(msgStream, Encoding.Unicode);

            byte[] data = reader.ReadBytes(msgIdSize);
            uint msgID;
            if (reverseByte)
                msgID = NetUtilites.ByteReverse_uint(data);
            else
                msgID = BitConverter.ToUInt32(data, 0);
            data = reader.ReadBytes(thepacket.Length - msgIdSize);
            MsgObject msg = new MsgObject((OpCode)msgID, data);
            return msg;
            

        }

        protected byte[] GetReliableMsg()
        {
            if (_recvBuffer == null)
                return null;

            //Queue is empty
            if (_recvBuffer.Count <= 0)
            {
                return null;
            }

            int MaxPiece = _recvBuffer.Peek().MaxPiece;
            //No enough pieces
            if (_recvBuffer.Count < MaxPiece)
            {
                //Debug.Log(string.Format("Not Enough Packet, Need: {0}, Queue: {1}, Assembling: {2}", package.MaxPiece, _assmblingPackages.Count, _recvQueue.Count));
                return null;
            }

            int dataLength = 0;
            List<byte[]> resultData = new List<byte[]>();
            //Debug.Log("MaxPiece: " + MaxPiece.ToString());
            for (int i = 0; i < MaxPiece; i++)
            {
                RecvingPackage apackage = _recvBuffer.Dequeue();
                /*StringBuilder sb = new StringBuilder();
                sb.Append("getPackage: ");
                for (int j = 0; j < apackage.Data.Length; j++)
                {
                    sb.Append(apackage.Data[j] + ", ");
                }
                Debug.Log(sb.ToString());*/
                resultData.Add(apackage.Data);
                dataLength += apackage.Data.Length;
            }

            byte[] ret = new byte[dataLength];
            int currentPos = 0;
            for (int i = 0; i < resultData.Count; i++)
            {
                byte[] Data = resultData[i];
                Data.CopyTo(ret, currentPos);
                currentPos += Data.Length;
            }
            return ret;
        }
#endregion

#region SendMessage
        public void SendMessage(OpCode f_id, byte[] f_buf)
        {
            SendMsg((uint)f_id, f_buf);
        }

        private void SendMsg(uint f_id, byte[] f_buf)
        {
            if (reverseByte)
                f_id = NetUtilites.ByteReverse_uint(f_id); 

            byte[] data = new byte[f_buf.Length + msgIdSize];
            BitConverter.GetBytes(f_id).CopyTo(data, 0);

            if (f_buf != null)
                f_buf.CopyTo(data, msgIdSize);
            SendReliable(data);
        }

        protected void SendReliable(byte[] data)
        {
            //Capsule New Data
            List<SendingPackage> packages = Capsule(data);
            for (int i = 0; i < packages.Count; i++)
                _sendQueue.Enqueue(packages[i]);
        }

        private List<SendingPackage> Capsule(byte[] data)
        {
            List<SendingPackage> ret = new List<SendingPackage>();
            int frameNum = (data.Length + (MTU - DataFrameHeaderLength - PublicFrameHeaderLength) - 1) / (MTU - DataFrameHeaderLength - PublicFrameHeaderLength);
            ushort currentPosition = 0;
            for (int i = 0; i < frameNum; i++)
            {
                ushort dataLength = (ushort)Math.Min(MTU - DataFrameHeaderLength - PublicFrameHeaderLength, data.Length - currentPosition);
                byte[] frame = new byte[dataLength + DataFrameHeaderLength];
                MemoryStream ms = new MemoryStream(frame);
                BinaryWriter bw = new BinaryWriter(ms);
                byte[] lenBytes = BitConverter.GetBytes((ushort)(dataLength + 7));
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(lenBytes);
                bw.Write(lenBytes);//0 len
                bw.Write((byte)4);//2 control
                byte[] currentSendSeqBytes = BitConverter.GetBytes(_currentSnedSeq);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(currentSendSeqBytes);
                bw.Write(currentSendSeqBytes);//3 seqNo
                byte[] maxPieceBytes = BitConverter.GetBytes((ushort)frameNum);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(maxPieceBytes);
                bw.Write(maxPieceBytes);//7 max piece
                bw.Write(data, currentPosition, dataLength);//9 data
                bw.Close();
                SendingPackage package = new SendingPackage();

                package.Content = frame;
                package.SendingSequenceNo = _currentSnedSeq;
                package.fastack = 0;
                package.RetransmissionInterval = RTO;
                _currentSnedSeq++;
                currentPosition += dataLength;
                ret.Add(package);
            }
            return ret;
        }
#endregion

#region socket
        protected virtual void Connect(IPEndPoint remoteIp)
        {

        }

        protected virtual bool Send(byte[] data, int len)
        {
            byte[] sessionBytes = BitConverter.GetBytes(SessionID);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(sessionBytes);
            Array.Copy(sessionBytes, 0, data, 2, 4);

            byte[] unaBytes = BitConverter.GetBytes(_una);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(unaBytes);
            Array.Copy(unaBytes, 0, data, 6, 4);

            UInt16 checksum = CRCCheck.crc16(data, 2, len);
            byte[] checksumBytes = BitConverter.GetBytes(checksum);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(checksumBytes);
            Array.Copy(checksumBytes, 0, data, 0, 2);
            return false;
        }

        protected virtual bool Recv(ref byte[] data, ref int len)
        {
            byte[] checksumBytes = new byte[2] { data[0], data[1] };
            if (BitConverter.IsLittleEndian)
                Array.Reverse(checksumBytes);
            UInt16 checksum = BitConverter.ToUInt16(checksumBytes, 0);
            UInt16 calChecksum = CRCCheck.crc16(data, 2, len);
            if (checksum != calChecksum)
            {
                data = null;
                len = 0;
            }
            return true;
        }

        protected virtual void Close()
        {

        }
#endregion
    }

    public class RUDPConnection : RUDP
    {
        private Socket m_socket;

        readonly byte[] _recvBuffer = new byte[2000];

        private IPEndPoint FarEnd;

        protected override bool Send(byte[] f_data, int len)
        {
            base.Send(f_data, len);
            if (m_socket == null || FarEnd == null)
                return false;
            try
            {
                /*StringBuilder sb = new StringBuilder();
                sb.Append("Send: ");
                for(int i = 0; i < len; i++)
                {
                    sb.Append(f_data[i] + ", ");
                }
                Debug.Log(sb.ToString());*/

                int n = m_socket.SendTo(f_data, len, SocketFlags.None, FarEnd);
            }
            catch(SocketException e)
            {
            }
            return true;
        }
        
        protected override bool Recv(ref byte[] data, ref int len)
        {
            data = null;
            len = 0;
            if (m_socket == null || FarEnd == null)
                return false;
            try
            {
                EndPoint recvEndPoint = (EndPoint)FarEnd;
                int n = m_socket.ReceiveFrom(_recvBuffer, ref recvEndPoint);
                /*StringBuilder sb = new StringBuilder();
                sb.Append("Recv: ");
                for (int i = 0; i < n; i++)
                {
                    sb.Append(_recvBuffer[i] + ", ");
                }
                Debug.Log(sb.ToString());*/
                data = _recvBuffer;
                len = n;
            }
            catch(SocketException e)
            {
                return false;
            }

            return base.Recv(ref data, ref len);
        }

        protected override void Close()
        {
            if (m_socket != null)
            {
                m_socket = null;
            }
        }

        protected override void Connect(IPEndPoint remoteIp)
        {
            Close();

            m_socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            m_socket.Blocking = false;
            m_socket.DontFragment = true;
            m_socket.SendBufferSize = 81920;
            m_socket.ReceiveBufferSize = 81920;
            FarEnd = remoteIp;
        }

    }
}