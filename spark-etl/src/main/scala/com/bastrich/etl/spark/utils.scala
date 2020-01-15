package com.bastrich.etl.spark

import org.jboss.netty.handler.ipfilter.IpSubnet

object utils {
  val isIpOfSubnet: (String, String) => Boolean =
    (ip, subnet) => new IpSubnet(subnet).contains(ip)
}
