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
        public bool IsWaitingAck;
        public UInt32 SendingSequenceNo;
        public DateTime LastSendTimestamp;
        public DateTime FirstSendTimestamp;
        public bool IsSending = false;
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
        public static ushort MTU = 1459;
        public uint RetransmissionInterval = 100;//in millisecond
        public int MultiSend = 1;

        static readonly public int msgIdSize = 4;
        public bool reverseByte = true;

        public ushort MaxWaitingSendLength = 100;
        private Queue<SendingPackage> _sendQueue;
        private List<SendingPackage> _waitAckList;

        private List<UInt32> _sendAckList;

        private Dictionary<UInt32, RecvingPackage> _recvQueue;
        private UInt32 _nextRecvSeqNo;
        private UInt32 _lastRecvSeqNo = UInt32.MaxValue;
        private UInt32 _una;

        private UInt32 _currentSnedSeq;

        private const ushort DataFrameHeaderLength = 104/8;

        private List<RecvingPackage> _assmblingPackages;

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
            _waitAckList = new List<SendingPackage>();

            _sendAckList = new List<UInt32>();

            _recvQueue = new Dictionary<UInt32, RecvingPackage>();
            _assmblingPackages = new List<RecvingPackage>();
            RUDPReset();
        }

        public void RUDPReset()
        {
            _sendQueue.Clear();
            _waitAckList.Clear();

            _sendAckList.Clear();

            _recvQueue.Clear();
            _assmblingPackages.Clear();
               
            _nextRecvSeqNo = 0;
            _una = _nextRecvSeqNo;
            _currentSnedSeq = 0;

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
            MobaNetworkManager.Instance.waitingRecvNum = _recvQueue.Count + _assmblingPackages.Count;

            ProcessSendQueue(deltaTime);
        }

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
                FlushAck();

                //Put available packages to waiting dict
                int readyToSendNum = 0;

                readyToSendNum = Math.Min(MaxWaitingSendLength - _waitAckList.Count, _sendQueue.Count);
                for (int i = 0; i < readyToSendNum; i++)
                {
                    SendingPackage package = _sendQueue.Dequeue();
                    Array.Copy(BitConverter.GetBytes(_una), 0, package.Content, 9, 4);
                    bool success = Send(package.Content);
                    package.LastSendTimestamp = DateTime.Now;
                    package.FirstSendTimestamp = DateTime.Now;
                    _waitAckList.Add(package);
                    if (!success)
                        return;
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
                        Array.Copy(BitConverter.GetBytes(_una), 0, package.Content, 9, 4);
                        if (!Send(package.Content))
                            return;
                        package.LastSendTimestamp = DateTime.Now;
                        package.fastack = 0;
                    }
                }
            }
            MobaNetworkManager.Instance.waitingSendNum = _sendQueue.Count + _waitAckList.Count;
        }

        //DateTime lastTime;
        void ProcessRecvQueue(byte[] rawData, int len)
        {    
            MemoryStream msgStream = new MemoryStream(rawData);
            BinaryReader reader = new BinaryReader(msgStream);
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
                byte[] unaBytes = reader.ReadBytes(4);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(unaBytes);
                uint coUna = BitConverter.ToUInt32(unaBytes, 0);
                byte[] data = reader.ReadBytes(len - DataFrameHeaderLength);                   
                if (!_recvQueue.ContainsKey(seqData) && seqData >= _nextRecvSeqNo)
                {
                    //replace dummy packages
                    RecvingPackage recvPackage = new RecvingPackage();
                    recvPackage.Data = data;
                    recvPackage.MaxPiece = maxPiece;
                    recvPackage.RecvingSequenceNo = seqData;
                    _recvQueue.Add(seqData, recvPackage);
                }
                AppendAck(seqData);

                ProcessUna(coUna);
            }
            else if(control == (byte)3) //ACK, FIN+ACK
            {
                //this is a ack frame
                byte[] unaBytes = reader.ReadBytes(4);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(unaBytes);
                UInt32 coUna = BitConverter.ToUInt32(unaBytes, 0);
                int ackNum = reader.ReadByte();

                for(int i = 0; i < ackNum; i++)
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
                            if (MobaNetworkManager.Instance.pingQueue.Count > 10)
                            {
                                int oldPing = MobaNetworkManager.Instance.pingQueue.Dequeue();
                                MobaNetworkManager.Instance.ping -= oldPing;
                            }
                            int newPing = (int)(DateTime.Now - sendPackage.FirstSendTimestamp).TotalMilliseconds;
                            MobaNetworkManager.Instance.pingQueue.Enqueue(newPing);
                            MobaNetworkManager.Instance.ping += newPing;
                            _waitAckList.Remove(sendPackage);

                            for (int j = 0; j <= _waitAckList.Count; j++)
                            {
                                if (_waitAckList[j].SendingSequenceNo < seqData)
                                {
                                    _waitAckList[j].fastack++;
                                }
                            }
                        }
                    }
                    else if (_state == RUDP_STATE.LAST_ACK)
                    {
                        if (seqData == _currentSnedSeq)
                            RUDPReset();
                    }
                }

                if(_state == RUDP_STATE.ESTABLISED)
                {
                    ProcessUna(coUna);
                }
            } 
            else if(control == (byte) 2)//SYN+ACK
            {
                byte[] seqDataBytes = reader.ReadBytes(4);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(seqDataBytes);
                UInt32 seqData = BitConverter.ToUInt32(seqDataBytes, 0);
                AppendAck(seqData);
                _state = RUDP_STATE.ESTABLISED;
                //lastTime = DateTime.Now;
                _nextRecvSeqNo = 1;
                _lastRecvSeqNo = UInt32.MaxValue;
            }
            else if(control == (byte) 6)//FIN
            {
                byte[] seqDataBytes = reader.ReadBytes(4);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(seqDataBytes);
                _lastRecvSeqNo = BitConverter.ToUInt32(seqDataBytes, 0);
            }
            else if(control == (byte) 5)//RST
            {
                OnRUDPConnectionDisconnect();
            }
            else
            {
                Debug.LogError("Receive Illegal Package");
            }
        }

        void ProcessUna(uint coUna)
        {
            for (int i = _waitAckList.Count - 1; i >= 0; i--)
            {
                if (_waitAckList[i].SendingSequenceNo <= coUna)
                {
                    _waitAckList.RemoveAt(i);
                }
            }
        }

        void AppendAck(UInt32 seqNo)
        {
            _sendAckList.Add(seqNo);
        }

        byte[] AckFrame = new byte[MTU];

        private void FlushAck()
        {
            //if ack list is empty, return

            //Calculate una
            _una = _nextRecvSeqNo;
            while(true)
            {
                if(_recvQueue.ContainsKey(_una + 1))
                {
                    _una++;
                }
                else
                {
                    break;
                }
            }

            //delete ack < _una
            for(int i = _sendAckList.Count - 1; i >= 0; i--)
            {
                if(_sendAckList[i] <= _una)
                {
                    _sendAckList.RemoveAt(i);
                }
            }

            MemoryStream ms = new MemoryStream(AckFrame);
            BinaryWriter bw = new BinaryWriter(ms);
            bw.Write((ushort)0);//0
            bw.Write((byte)3);//2
            bw.Write(_una);//3
            bw.Write((byte)0);//7
            int lastRow = 0;

            //一个ACK包最多承载255个ack
            for (int i = 0; i < _sendAckList.Count; i++)
            {
                byte[] AckSeqBytes = BitConverter.GetBytes(_sendAckList[i]);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(AckSeqBytes);
                bw.Write(AckSeqBytes); 
                if((i - lastRow) == 255 || i == _sendAckList.Count - 1)
                {
                    bw.Close();
                    AckFrame[7] = (byte)(i - lastRow);
                    lastRow = i;
                    if (!Send(AckFrame))
                        break;

                    ms = new MemoryStream(AckFrame);
                    bw = new BinaryWriter(ms);
                    bw.Write((ushort)0);
                    bw.Write((byte)3);
                    bw.Write(_una);
                    bw.Write((byte)0);
                }

                bw.Close();
            }
        }

        private void SendSYN(byte[] cookie)
        {
            Debug.Log("SendSYN");
            byte[] frame = new byte[3 + cookie.Length];
            MemoryStream ms = new MemoryStream(frame);
            BinaryWriter bw = new BinaryWriter(ms);
            bw.Write((ushort)0);
            bw.Write((byte)1);

            bw.Write(cookie);
            bw.Close();

            //Debug.Log("Send SYN: ");
            Send(frame);
        }

        private void SendFINACK()
        {
            byte[] frame = new byte[7];
            MemoryStream ms = new MemoryStream(frame);
            BinaryWriter bw = new BinaryWriter(ms);
            bw.Write((ushort)0);
            bw.Write((byte)7);
            byte[] AckSeqBytes = BitConverter.GetBytes(_currentSnedSeq);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(AckSeqBytes);
            bw.Write(AckSeqBytes);
            
            bw.Close();

            //Debug.Log("Send FINACK");
            Send(frame);
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

            if (_nextRecvSeqNo == _lastRecvSeqNo)
            {
                SendFINACK();
                _state = RUDP_STATE.LAST_ACK;
            }

            if (_recvQueue == null)
                return null;

            //Queue is empty
            if (_recvQueue.Count <= 0)
            {
                return null;
            }
            
            if (!_recvQueue.ContainsKey(_nextRecvSeqNo))
                return null;
            RecvingPackage package = _recvQueue[_nextRecvSeqNo];
            //No enough pieces
            if (_recvQueue.Count + _assmblingPackages.Count < package.MaxPiece)
            {
                //Debug.Log(string.Format("Not Enough Packet, Need: {0}, Queue: {1}, Assembling: {2}", package.MaxPiece, _assmblingPackages.Count, _recvQueue.Count));
                return null;
            }


            //put non-dummy frames to assemble list
            int assemblingPackagesNum = _assmblingPackages.Count;
            //Debug.Log("MaxPiece: " + package.MaxPiece.ToString());
            for (int i = 0; i < package.MaxPiece - assemblingPackagesNum; i++)
            {
                if (!_recvQueue.ContainsKey(_nextRecvSeqNo))
                    break;
                RecvingPackage apackage = _recvQueue[_nextRecvSeqNo];
                _recvQueue.Remove(apackage.RecvingSequenceNo);
                _assmblingPackages.Add(apackage);
                _nextRecvSeqNo++;
            }

            if (_assmblingPackages.Count != package.MaxPiece)
                return null;

            //calculate data length
            int dataLength = 0;
            foreach (RecvingPackage apackage in _assmblingPackages)
            {
                dataLength += apackage.Data.Length;
            }

            byte[] ret = new byte[dataLength];
            int currentPos = 0;
            foreach (RecvingPackage apackage in _assmblingPackages)
            {
                apackage.Data.CopyTo(ret, currentPos);
                currentPos += apackage.Data.Length;
            }
            _assmblingPackages.Clear();
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
            foreach (SendingPackage package in packages)
                _sendQueue.Enqueue(package);
        }

        private List<SendingPackage> Capsule(byte[] data)
        {
            List<SendingPackage> ret = new List<SendingPackage>();
            int frameNum = (data.Length + (MTU - DataFrameHeaderLength) - 1) / (MTU - DataFrameHeaderLength);
            ushort currentPosition = 0;
            for (int i = 0; i < frameNum; i++)
            {
                ushort dataLength = (ushort)Math.Min(MTU - DataFrameHeaderLength, data.Length - currentPosition);
                byte[] frame = new byte[dataLength + DataFrameHeaderLength];
                MemoryStream ms = new MemoryStream(frame);
                BinaryWriter bw = new BinaryWriter(ms);
                bw.Write(BitConverter.GetBytes((ushort)0));//0
                bw.Write((byte)4);//2
                byte[] currentSendSeqBytes = BitConverter.GetBytes(_currentSnedSeq);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(currentSendSeqBytes);
                bw.Write(currentSendSeqBytes);//3
                byte[] maxPieceBytes = BitConverter.GetBytes((ushort)frameNum);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(maxPieceBytes);
                bw.Write(maxPieceBytes);//7
                bw.Write((uint)0);//9
                bw.Write(data, currentPosition, dataLength);//13
                bw.Close();
                SendingPackage package = new SendingPackage();

                

                package.Content = frame;
                package.IsWaitingAck = true;
                package.SendingSequenceNo = _currentSnedSeq;
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

        protected virtual bool Send(byte[] data)
        {
            UInt16 checksum = CRCCheck.crc16(data, 2, data.Length);
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

        protected override bool Send(byte[] f_data)
        {
            base.Send(f_data);
            if (m_socket == null)
                return false;
            try
            {
                for (int i = 0; i < MultiSend; i++)
                {
                    m_socket.Send(f_data);
                }
            }
            catch(SocketException e)
            {
                if (!_socketSendErrorSet.Contains(e.SocketErrorCode.ToString()))
                {
                    Debug.Log("RUDP Send Exception: " + e.SocketErrorCode.ToString());
                    _socketSendErrorSet.Add(e.SocketErrorCode.ToString());
                }

                if (e.SocketErrorCode != SocketError.WouldBlock)
                {
                    OnRUDPConnectionDisconnect();
                    return false;
                }
                    
            }
            return true;
        }
        
        protected override void Recv(ref byte[] data, ref int len)
        {
            if (m_socket == null)
                return;
            try
            {
                int n = m_socket.Receive(_recvBuffer);
                data = _recvBuffer;
                len = n;
            }
            catch(SocketException e)
            {
                if (!_socketRecvErrorSet.Contains(e.SocketErrorCode.ToString()))
                {
                    Debug.Log("RUDP Recv Exception: " + e.SocketErrorCode.ToString());
                    _socketRecvErrorSet.Add(e.SocketErrorCode.ToString());
                }

                if (e.SocketErrorCode != SocketError.WouldBlock && e.SocketErrorCode != SocketError.ConnectionReset)
                {
                    OnRUDPConnectionDisconnect();
                }
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