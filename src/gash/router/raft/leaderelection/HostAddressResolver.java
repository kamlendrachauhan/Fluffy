package gash.router.raft.leaderelection;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class HostAddressResolver {

	public static String getLocalHostAddress() {

		String interfaceName1 = "eth0";
		String interfaceName2 = "en0";
		String interfaceName3 = "enp4s0";
		String ip = "";
		NetworkInterface networkInterface;
		try {
			networkInterface = NetworkInterface.getByName(interfaceName1);
			if (networkInterface == null) {
				networkInterface = NetworkInterface.getByName(interfaceName2);
			}
			if (networkInterface == null) {
				networkInterface = NetworkInterface.getByName(interfaceName3);
			}
			Enumeration<InetAddress> inetAddress = networkInterface.getInetAddresses();
			InetAddress currentAddress;
			currentAddress = inetAddress.nextElement();
			while (inetAddress.hasMoreElements()) {
				currentAddress = inetAddress.nextElement();
				if (currentAddress instanceof Inet4Address && !currentAddress.isLoopbackAddress()) {
					ip = currentAddress.toString();
					break;
				}
			}
		} catch (SocketException e) {
			// TODO put logger
			System.out.println("An Error has occured while finding out local address ");
		} catch (Exception e) {
			System.out.println("An error has occured while resolving address");
			return "";

		}
		return (ip.replace("/", ""));

	}
}
