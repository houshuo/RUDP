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
        public DateTime LastSendTimestamp;
        public DateTime FirstSendTimestamp;
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
        public static ushort MTU = 1464;
        public uint RetransmissionInterval = 100;//in millisecond

        static readonly public int msgIdSize = 4;
        public bool reverseByte = true;

        public ushort MaxWaitingSendLength = 100;
        private Queue<SendingPackage> _sendQueue;//all input waiting here
        private Queue<byte[]> _sendBuffer;//data in stream
        private List<SendingPackage> _waitAckList;

        private List<RecvingPackage> _recvQueue;//data in stream
        private Queue<RecvingPackage> _recvBuffer;//wait to get
        private const int MaxRecvWindSize = 100;
        private UInt32 _lastRecvSeqNo = UInt32.MaxValue;
        private UInt32 _una;

        private UInt32 _currentSnedSeq;

        private const ushort DataFrameHeaderLength = 72 / 8;
        private const ushort PublicFrameHeaderLength = 48 / 8;

        protected RUDP_STATE _state;
        float _stateTimer;

        const float SynReceivedTimeout = 2;
        const float LastAckTimeout = 5;
        const float AckTimeout = 10;

        protected enum RUDP_STATE
        {
            CLOSED,
            SYN_SEND,
            ESTABLISED,
            LAST_ACK
        }

        enum PACKAGE_CATE
        {
            DATA = 4,
            ACK = 3,
            FIN_ACK = 2,
            FIN = 6,
            RST = 5
        }

        public RUDP()
        {
            _sendQueue = new Queue<SendingPackage>();
            _sendBuffer = new Queue<byte[]>();
            _waitAckList = new List<SendingPackage>();

            _recvQueue = new List<RecvingPackage>(MaxWaitingSendLength);
            _recvBuffer = new Queue<RecvingPackage>();
            RUDPReset();
        }

        public void RUDPReset()
        {
            _sendQueue.Clear();
            _sendBuffer.Clear();
            _waitAckList.Clear();

            _recvQueue.Clear();
            for (int i = 0; i < MaxWaitingSendLength; i++)
            {
                _recvQueue.Add(null);
            }
            _recvBuffer.Clear();
               
            _una = 0;
            _currentSnedSeq = 1;

            _state = RUDP_STATE.CLOSED;
            _stateTimer = 0;
            Close();
        }

        public bool RUDPConnect(IPEndPoint remoteIp, byte[] cookie)
        {
            if (_state == RUDP_STATE.SYN_SEND || _state == RUDP_STATE.CLOSED)
            {
                if(_state == RUDP_STATE.SYN_SEND)
                    RUDPReset();
                Connect(remoteIp);
                SendSYN(cookie);
                _state = RUDP_STATE.SYN_SEND;
                _stateTimer = 0;
                return true;
            }  
            else 
                return false;
        }

        public void Tick(float deltaTime)
        {
            byte[] rawData = null;
            int len = 0;
            Recv(ref rawData, ref len);
            while (rawData != null && len > 0)
            {
                ProcessRecvQueue(rawData, len);
                Recv(ref rawData, ref len);
            }
            MobaNetworkManager.Instance.waitingRecvNum = _recvQueue.Count + _recvBuffer.Count;

            ProcessSendQueue(deltaTime);
        }

        byte[] SendStreamBuffer = new byte[MTU]; // for final frame construction 
        void ProcessSendQueue(float deltaTime)
        {
            if (_state == RUDP_STATE.SYN_SEND)
            {
                _stateTimer += deltaTime;
                if (_stateTimer > SynReceivedTimeout)
                {
                    OnRUDPConnectionDisconnect();
                }
            }
            else if (_state == RUDP_STATE.LAST_ACK)
            {
                _stateTimer += deltaTime;
                if (_stateTimer > LastAckTimeout)
                {
                    SendFINACK();
                    _stateTimer = 0;
                }

            }
            else if (_state == RUDP_STATE.ESTABLISED)
            {
                //Put available packages to waiting dict
                int readyToSendNum = 0;

                readyToSendNum = Math.Min(MaxWaitingSendLength - _waitAckList.Count, _sendQueue.Count);
                for (int i = 0; i < readyToSendNum; i++)
                {
                    SendingPackage package = _sendQueue.Dequeue();
                    _sendBuffer.Enqueue(package.Content);
                    package.LastSendTimestamp = DateTime.Now;
                    package.FirstSendTimestamp = DateTime.Now;
                    _waitAckList.Add(package);
                }

                //Re-send un-acked packages
                //string pendingList = "";
                for (int i = 0; i < _waitAckList.Count; i++)
                {
                    SendingPackage package = _waitAckList[i];
                    if ((DateTime.Now - package.FirstSendTimestamp).Seconds > AckTimeout)
                    {
                        OnRUDPConnectionDisconnect();
                        return;
                    }

                    if ((DateTime.Now - package.LastSendTimestamp).Milliseconds > RetransmissionInterval || package.fastack >= 2)
                    {
                        _sendBuffer.Enqueue(package.Content);
                        package.LastSendTimestamp = DateTime.Now;
                        package.fastack = 0;
                    }
                }
            }
            MobaNetworkManager.Instance.waitingSendNum = _sendQueue.Count + _waitAckList.Count;

            int currentPos = PublicFrameHeaderLength;
            //actually send
            while (_sendBuffer.Count > 0)
            {
                byte[] nextSendContent = _sendBuffer.Dequeue();
                if (currentPos + nextSendContent.Length > MTU)
                {
                    Send(SendStreamBuffer, currentPos);
                    currentPos = PublicFrameHeaderLength;
                }
                Array.Copy(nextSendContent, 0, SendStreamBuffer, currentPos, nextSendContent.Length);
                currentPos += nextSendContent.Length;
            }
            if(currentPos > PublicFrameHeaderLength)
            {
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
            if (BitConverter.IsLittleEndian)
                Array.Reverse(checksumBytes);
            UInt16 checksum = BitConverter.ToUInt16(checksumBytes, 0);
            UInt16 calChecksum = CRCCheck.crc16(rawData, 2, len);
            if (checksum != calChecksum)
            {
                Debug.LogWarning("Checksum Failed!!!!!!");
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
                if (control == (byte)4)
                {
                    if (_state != RUDP_STATE.ESTABLISED)
                        return;
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
                else if (control == (byte)3) //ACK, FIN+ACK
                {
                    byte[] seqDataBytes = reader.ReadBytes(4);
                    if (BitConverter.IsLittleEndian)
                        Array.Reverse(seqDataBytes);
                    UInt32 seqData = BitConverter.ToUInt32(seqDataBytes, 0);
                    //Debug.Log("Recv Ack SeqNo: " + seqData.ToString());
                    if (_state == RUDP_STATE.ESTABLISED)
                    {
                        SendingPackage sendPackage = _waitAckList.Find((SendingPackage input) => input.SendingSequenceNo == seqData);
                        if (sendPackage != null)
                        {
                            _waitAckList.Remove(sendPackage);

                            if(maxAckSeq < seqData)
                            {
                                maxAckSeq = seqData;
                            }
                        }
                    }
                    else if (_state == RUDP_STATE.LAST_ACK)
                    {
                        if (seqData == _currentSnedSeq)
                            RUDPReset();
                    }
                }
                else if (control == (byte)2)//SYN+ACK
                {
                    byte[] seqDataBytes = reader.ReadBytes(4);
                    if (BitConverter.IsLittleEndian)
                        Array.Reverse(seqDataBytes);
                    UInt32 seqData = BitConverter.ToUInt32(seqDataBytes, 0);
                    SendAck(seqData);
                    _una++;
                    _state = RUDP_STATE.ESTABLISED;
                    //lastTime = DateTime.Now;
                    _lastRecvSeqNo = UInt32.MaxValue;
                }
                else if (control == (byte)6)//FIN
                {
                    byte[] seqDataBytes = reader.ReadBytes(4);
                    if (BitConverter.IsLittleEndian)
                        Array.Reverse(seqDataBytes);
                    _lastRecvSeqNo = BitConverter.ToUInt32(seqDataBytes, 0);
                }
                else if (control == (byte)5)//RST
                {
                    OnRUDPConnectionDisconnect();
                }
                else
                {
                    Debug.LogError("Receive Illegal Package");
                }
            }

            if (_state == RUDP_STATE.ESTABLISED)
            {
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

        private void SendSYN(byte[] cookie)
        {
            Debug.Log("SendSYN");
            byte[] frame = new byte[3 + cookie.Length];
            MemoryStream ms = new MemoryStream(frame);
            BinaryWriter bw = new BinaryWriter(ms);
            byte[] lenBytes = BitConverter.GetBytes((ushort)(1 + cookie.Length));
            if (BitConverter.IsLittleEndian)
                Array.Reverse(lenBytes);
            bw.Write(lenBytes);//0 len
            bw.Write((byte)1);//control 2

            bw.Write(cookie);//cookie 3
            bw.Close();

            //Debug.Log("Send SYN: ");
            _sendBuffer.Enqueue(frame);
        }

        private void SendFINACK()
        {
            byte[] frame = new byte[7];
            MemoryStream ms = new MemoryStream(frame);
            BinaryWriter bw = new BinaryWriter(ms);
            byte[] lenBytes = BitConverter.GetBytes((ushort)(5));
            if (BitConverter.IsLittleEndian)
                Array.Reverse(lenBytes);
            bw.Write(lenBytes);//0 len
            bw.Write((byte)7);//control 2
            byte[] AckSeqBytes = BitConverter.GetBytes(_currentSnedSeq);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(AckSeqBytes);
            bw.Write(AckSeqBytes);//seqNo 3
            
            bw.Close();

            //Debug.Log("Send FINACK");
            _sendBuffer.Enqueue(frame);
        }

        #region IsConnected
        public bool IsConnected()
        {
            return _state == RUDP_STATE.ESTABLISED;
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

            if (_una == _lastRecvSeqNo)
            {
                SendFINACK();
                _state = RUDP_STATE.LAST_ACK;
            }

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
            byte[] unaBytes = BitConverter.GetBytes(_una);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(unaBytes);
            Array.Copy(unaBytes, 0, data, 2, 4);

            UInt16 checksum = CRCCheck.crc16(data, 2, len);
            byte[] checksumBytes = BitConverter.GetBytes(checksum);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(checksumBytes);
            Array.Copy(checksumBytes, 0, data, 0, 2);
            return false;
        }

        protected virtual void Recv(ref byte[] data, ref int len)
        {

        }

        protected virtual void Close()
        {

        }

        protected virtual void OnRUDPConnectionDisconnect()
        {
            Debug.Log("OnRUDPConnectionDisconnect");
            RUDPReset();
        }
        #endregion
    }

    public class RUDPConnection : RUDP
    {
        private Socket m_socket;

        readonly byte[] _recvBuffer = new byte[MTU];

        public event Action OnDisconnect;

        HashSet<string> _socketSendErrorSet = new HashSet<string>();
        HashSet<string> _socketRecvErrorSet = new HashSet<string>();

        protected override bool Send(byte[] f_data, int len)
        {
            base.Send(f_data, len);
            if (m_socket == null)
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
                m_socket.Send(f_data, len, SocketFlags.None);
                
            }
            catch(SocketException e)
            {
                    
            }
            return true;
        }
        
        protected override void Recv(ref byte[] data, ref int len)
        {
            data = null;
            len = 0;
            if (m_socket == null)
                return;
            try
            {
                int n = m_socket.Receive(_recvBuffer);
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

            }
        }

        protected override void OnRUDPConnectionDisconnect()
        {
            base.OnRUDPConnectionDisconnect();
            OnDisconnect();
        }

        protected override void Close()
        {
            if (m_socket != null)
            {
                m_socket.Close();
                m_socket = null;
            }
        }

        protected override void Connect(IPEndPoint remoteIp)
        {
            Close();
            Socket newSocket = null;
            try
            {
                newSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
                newSocket.Blocking = false;
                newSocket.DontFragment = true;
                newSocket.SendBufferSize = 81920;
                newSocket.ReceiveBufferSize = 81920;
                newSocket.Connect(remoteIp);
            }
            catch (SocketException e)
            {
                if (e.SocketErrorCode != SocketError.WouldBlock)
                {
                    return;
                }
            }
            m_socket = newSocket;
        }

    }
}