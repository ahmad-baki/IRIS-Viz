using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetMQ;
using NetMQ.Sockets;
using UnityEngine;
using System.Net;
using System.Net.Sockets;
using IRIS.Utilities;
using Unity.VisualScripting;
using System.Net.NetworkInformation;

namespace IRIS.Node
{

	public class IRISNetManager : MonoBehaviour
	{
		// Singleton instance
		public static IRISNetManager Instance { get; private set; }
		// Node information
		public NodeInfo localInfo { get; set; }
		// public NodeInfo masterInfo { get; set; }
		// public NodeInfoManager nodeInfoManager { get; private set; }
		// UDP Task management
		private CancellationTokenSource cancellationTokenSource;
		private Task nodeTask;
		// Lock for updating action in the main thread
		private object updateActionLock = new();
		public Action OnConnectionStart;
		public Action OnDisconnected;
		public ResponseSocket _resSocket;
		public Dictionary<string, Func<byte[], byte[]>> serviceCallbacks { get; private set; }
		// subscriber socket for receiving messages from only master node
		private SubscriberSocket _subSocket;
		// publisher socket for sending messages to other nodes
		public PublisherSocket _pubSocket;
		public Dictionary<string, Action<byte[]>> subscribeCallbacks { get; private set; }
		private List<NetMQSocket> _sockets;
		public Action ConnectionSpin;
		// Status flags
		private bool isRunning = false;
		private bool isConnected = false;
		// Constants
		private const int HEARTBEAT_INTERVAL = 500;
		private const string MCAST_ADDR = "239.192.1.1";
		private static readonly byte[] DISCOVERY_MSG = Encoding.UTF8.GetBytes("IRIS");
		private Service<string, string> renameService;

		#region CLIENT_CODE
		// // Request socket for sending service request to only master node
		// private RequestSocket _reqSocket;
		// ZMQ Sockets for communication, in this stage, we run them in the main thread
		// response socket for service running in the local node
		#endregion

		private void Awake()
		{
			// Singleton pattern
			if (Instance != null && Instance != this)
			{
				Destroy(gameObject);
				return;
			}
			Instance = this;
			DontDestroyOnLoad(gameObject);
			// Force to use .NET implementation of NetMQ
			AsyncIO.ForceDotNet.Force();
			// Initialize local node info
			localInfo = new NodeInfo
			{
				name = "UnityNode",
				nodeID = Guid.NewGuid().ToString(),
				addr = new NodeAddress("127.0.0.1", 0),
				type = "UnityNode",
				servicePort = UnityPortSet.SERVICE,
				topicPort = UnityPortSet.TOPIC,
				serviceList = new List<string>(),
				topicList = new List<string>()
			};
			// Default host name
			if (PlayerPrefs.HasKey("HostName"))
			{
				// The key exists, proceed to get the value
				string savedHostName = PlayerPrefs.GetString("HostName");
				localInfo.name = savedHostName;
				Debug.Log($"Find Host Name: {localInfo.name}");
			}
			else
			{
				// The key does not exist, handle it accordingly
				localInfo.name = "UnityNode";
				Debug.Log($"Host Name not found, using default name {localInfo.name}");
			}
			// NOTE: Since the NetZMQ setting is initialized in "AsyncIO.ForceDotNet.Force();"
			// NOTE: we should initialize the sockets after that
			_pubSocket = new PublisherSocket();
			_resSocket = new ResponseSocket();
			_subSocket = new SubscriberSocket();
			// _reqSocket = new RequestSocket();
			_sockets = new List<NetMQSocket>() { _resSocket, _subSocket, _pubSocket/*, _reqSocket*/ };
			serviceCallbacks = new();
			subscribeCallbacks = new();
			cancellationTokenSource = new CancellationTokenSource();
			// Action setting
			// OnConnectionStart += () => RunOnMainThread(() => StartConnection());
			OnConnectionStart += StartConnection;
			// OnDisconnected += () => RunOnMainThread(() => StopConnection());
			OnDisconnected += StopConnection;
			// Initialize the service callbacks
			renameService = new Service<string, string>("Rename", Rename, true);
		}

		private void Start()
		{
			// Start tasks
			Debug.Log("Starting node task...");
			isRunning = true;
			nodeTask = Task.Run(async () => await NodeTask(cancellationTokenSource.Token));
		}

		private void Update()
		{
			if (Monitor.TryEnter(updateActionLock))
			{
				try
				{
					ConnectionSpin?.Invoke();
				}
				finally
				{
					Monitor.Exit(updateActionLock);
				}
			}
		}

		private void OnApplicationQuit()
		{
			if (isConnected)
			{
				Debug.Log("Application is quitting.");
				// CallService<string, string>("NodeOffline", localInfo.nodeID);
			}
		}

		private void OnDestroy()
		{
			isConnected = false;
			isRunning = false;
			if (cancellationTokenSource != null)
			{
				cancellationTokenSource.Cancel();
				cancellationTokenSource.Dispose();
			}
			nodeTask?.Wait();
			StopConnection();
			foreach (var sock in _sockets)
			{
				sock?.Dispose();
			}
			NetMQConfig.Cleanup();
			Debug.Log("IRIS has been stopped safely.");
		}

		public void StartConnection()
		{
			if (isConnected) StopConnection();
			isConnected = true;
			lock (updateActionLock)
			{
				// subscription
				_subSocket.Bind($"tcp://{localInfo.addr.ip}:{UnityPortSet.TOPIC}");
				_subSocket.Subscribe("");
				Debug.Log($"Start subscribing to {localInfo.addr.ip}:{UnityPortSet.TOPIC}");
				// local service
				_resSocket.Bind($"tcp://{localInfo.addr.ip}:{UnityPortSet.SERVICE}");
				ConnectionSpin += SubscriptionSpin;
				ConnectionSpin += ServiceRespondSpin;
				Debug.Log($"Starting local service at {localInfo.addr.ip}:{UnityPortSet.SERVICE}");

				// local publish
				_pubSocket.Bind($"tcp://{localInfo.addr.ip}:{UnityPortSet.TOPIC}");
				Debug.Log($"Starting publish topic at {localInfo.addr.ip}:{UnityPortSet.TOPIC}");

				// // request to master node
				// _reqSocket.Connect($"tcp://{masterInfo.addr.ip}:{masterInfo.servicePort}");
				// Debug.Log($"Starting connecting to server at {masterInfo.addr.ip}:{masterInfo.servicePort}");
				// CalculateTimestampOffset();
				// CallService<NodeInfo, string>("RegisterNode", localInfo);

			}
		}

		public void StopConnection()
		{
			lock (updateActionLock)
			{
				while (_subSocket.HasIn) _subSocket.SkipFrame();
				ConnectionSpin = () => { };
				// It is not necessary to clear the topics callbacks
				// _topicsCallbacks.Clear();
				if (!isConnected) return;
				_resSocket.Unbind($"tcp://{localInfo.addr.ip}:{UnityPortSet.SERVICE}");
				_pubSocket.Unbind($"tcp://{localInfo.addr.ip}:{UnityPortSet.TOPIC}");
				// _reqSocket.Disconnect($"tcp://{masterInfo.addr.ip}:{masterInfo.servicePort}");
				_subSocket.Unbind($"tcp://{localInfo.addr.ip}:{UnityPortSet.TOPIC}");

			}
			Debug.Log("Stop connection");
			isConnected = false;
		}

		public async Task NodeTask(CancellationToken token)
		{
			Debug.Log("Node task starts and is sending Discovery messages...");

			using (var udp = new UdpClient())
			{
				var endpoint = new IPEndPoint(IPAddress.Parse(MCAST_ADDR), UnityPortSet.DISCOVERY);

				while (isRunning)
				{
					try
					{
						udp.Send(DISCOVERY_MSG, DISCOVERY_MSG.Length, endpoint);
						await Task.Delay(HEARTBEAT_INTERVAL, token);
					}
					catch (TaskCanceledException)
					{
						Debug.Log("Task is canceled by user");
						break;
					}
					catch (Exception e)
					{
						Debug.LogWarning(e.StackTrace);
					}
				}
			}
			Debug.Log("Node task ends");
		}

		public void SubscriptionSpin()
		{
			// Only process the latest message of each topic
			Dictionary<string, byte[]> messageProcessed = new();
			while (_subSocket.HasIn)
			{
				byte[][] msgSeparated = MsgUtils.SplitByte(_subSocket.ReceiveFrameBytes());
				string topic_name = MsgUtils.Bytes2String(msgSeparated[0]);
				if (subscribeCallbacks.ContainsKey(topic_name))
				{
					// Debug.Log($"Received message from {topic_name}");
					messageProcessed[topic_name] = msgSeparated[1];
				}
			}
			foreach (var (topic_name, msg) in messageProcessed)
			{
				subscribeCallbacks[topic_name](msg);
			}
		}

		public void ServiceRespondSpin()
		{
			if (!_resSocket.HasIn) return;
			// TODO: make it as a byte array
			// TODO: make it running in the sub thread
			// now we need to carefully handle the service request
			// make sure that it would not block the main thread
			byte[] messageReceived = _resSocket.ReceiveFrameBytes();
			byte[][] messageSplit = MsgUtils.SplitByte(messageReceived);
			string serviceName = MsgUtils.Bytes2String(messageSplit[0]);
			if (serviceCallbacks.ContainsKey(serviceName))
			{
				byte[] response = serviceCallbacks[serviceName](messageSplit[1]);
				_resSocket.SendFrame(response);
			}
			else
			{
				Debug.LogWarning($"Service {serviceName} not found");
				_resSocket.SendFrame(IRISSignal.NOSERVICE);
			}
		}


		// TODO: make it as a generic request type
		public string Rename(string newName)
		{
			localInfo.name = newName;
			PlayerPrefs.SetString("HostName", localInfo.name);
			Debug.Log($"Change Host Name to {localInfo.name}");
			PlayerPrefs.Save();
			return IRISSignal.SUCCESS;
		}

		/// <summary>
		/// Get the IP address of the WLAN interface.
		/// </summary>
		/// <returns>Returns the IPv4 address of the WLAN interface if found, otherwise returns null.</returns>
		/// /// <remarks>
		/// This is not used in the current implementation for safety reasons.
		/// </remarks>
		private static IPAddress GetWLANIpAdress()
		{
			NetworkInterface[] intf = NetworkInterface.GetAllNetworkInterfaces();
			foreach (NetworkInterface device in intf)
			{
				if (device.NetworkInterfaceType == NetworkInterfaceType.Wireless80211 && device.OperationalStatus == OperationalStatus.Up)
				{
					IPAddress ipv6Address = device.GetIPProperties().UnicastAddresses[0].Address; //This will give ipv6 address of certain adapter
					IPAddress ipv4Address = device.GetIPProperties().UnicastAddresses[1].Address; //This will give ipv4 address of certain adapter
					IPAddress unicastIPv4Mask = device.GetIPProperties().UnicastAddresses[1].IPv4Mask; //This will give ipv4 mask of certain adapter
					Debug.Log($"Found WLAN interface: {device.Name} with IPv4: {ipv4Address} and mask: {unicastIPv4Mask}");
					// Get the broadcast address for the IPv4 address
					return ipv4Address;
				}
			}
			Debug.LogError("No active WLAN interface found.");
			return null;
		}

		#region CLIENT_CODE
		// // TODO: make it as a generic request type
		// public byte[] CallBytesService(string service_name, string request)
		// {
		// 	_reqSocket.SendFrame($"{service_name}{MsgUtils.SEPARATOR}{request}");
		// 	if (!_reqSocket.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(10000), out byte[] bytes, out bool more))
		// 	{
		// 		Debug.LogWarning($"Request Timeout");
		// 		return new byte[] { };
		// 	}
		// 	List<byte> result = new List<byte>(bytes);
		// 	result.AddRange(bytes);
		// 	while (more) result.AddRange(_reqSocket.ReceiveFrameBytes(out more));
		// 	return result.ToArray();
		// }

		// public ResponseType CallService<RequestType, ResponseType>(string serviceName, RequestType request)
		// {
		// 	byte[] requestBytes;
		// 	if (typeof(RequestType) == typeof(string))
		// 	{
		// 		requestBytes = MsgUtils.String2Bytes((string)(object)request);
		// 	}
		// 	else
		// 	{
		// 		requestBytes = MsgUtils.Serialize2Byte(request);
		// 	}
		// 	byte[] responseBytes = CallBytesService(serviceName, Encoding.UTF8.GetString(requestBytes));
		// 	if (typeof(ResponseType) == typeof(string))
		// 	{
		// 		string result = Encoding.UTF8.GetString(responseBytes);
		// 		return (ResponseType)(object)result;
		// 	}
		// 	return MsgUtils.BytesDeserialize2Object<ResponseType>(responseBytes);
		// }
		#endregion
	}

}
