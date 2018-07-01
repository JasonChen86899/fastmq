package fastmq.client.consumer;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

/**
 * Author: Jason Chen Date: 2018/6/20
 */
public class LocalIpAddressUtil {

  public static String getConsumerIp(String ip) {
    Set<InetAddress> addressSet = resolveLocalAddresses();

    if (ip != null && addressSet.contains(ip)) {
      return ip;
    } else {
      return addressSet.stream().findAny().get().getHostAddress();
    }
  }

  private static Set<InetAddress> resolveLocalAddresses() {
    Set<InetAddress> addrs = new HashSet<>();
    Enumeration<NetworkInterface> ns = null;
    try {
      ns = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      e.printStackTrace();
    }
    while (ns != null && ns.hasMoreElements()) {
      NetworkInterface n = ns.nextElement();
      Enumeration<InetAddress> is = n.getInetAddresses();
      while (is.hasMoreElements()) {
        InetAddress i = is.nextElement();

        if (!i.isLoopbackAddress() && !i.isLinkLocalAddress() && !i.isMulticastAddress()
            && !isSpecialIp(i.getHostAddress())) {
          addrs.add(i);
        }
      }
    }

    return addrs;
  }


  private static boolean isSpecialIp(String ip) {
    if (ip.contains(":")) {
      return true;
    }
    if (ip.startsWith("127.")) {
      return true;
    }
    if (ip.startsWith("169.254.")) {
      return true;
    }
    if (ip.equals("255.255.255.255")) {
      return true;
    }
    return false;
  }

}
