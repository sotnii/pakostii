#![no_std]
#![no_main]

use aya_ebpf::{
    bindings::xdp_action,
    macros::{map, xdp},
    maps::{HashMap, RingBuf},
    programs::XdpContext,
};

use core::panic::PanicInfo;

const ETH_HDR_LEN: usize = 14;
const ETH_P_IP: u16 = 0x0800;
const ETH_P_ARP: u16 = 0x0806;
const ETH_TYPE_OFFSET: usize = 12;
const IPV4_SRC_OFFSET: usize = ETH_HDR_LEN + 12;
const IPV4_DST_OFFSET: usize = ETH_HDR_LEN + 16;
const IPV4_SRC_LEN: usize = 4;
const ARP_HTYPE_OFFSET: usize = ETH_HDR_LEN;
const ARP_PTYPE_OFFSET: usize = ETH_HDR_LEN + 2;
const ARP_HLEN_OFFSET: usize = ETH_HDR_LEN + 4;
const ARP_PLEN_OFFSET: usize = ETH_HDR_LEN + 5;
const ARP_SHA_OFFSET: usize = ETH_HDR_LEN + 8;
const ARP_SPA_OFFSET: usize = ETH_HDR_LEN + 14;
const ARP_TPA_OFFSET: usize = ETH_HDR_LEN + 24;
const ARP_HTYPE_ETHERNET: u16 = 1;
const ARP_HLEN_ETHERNET: u8 = 6;
const ARP_PLEN_IPV4: u8 = 4;
const DECISION_PASS: u8 = 1;
const DECISION_DROP: u8 = 2;

#[map]
static ALLOWED_SOURCES: HashMap<[u8; 4], u8> = HashMap::with_max_entries(256, 0);

#[map]
static PACKET_LOGS: RingBuf = RingBuf::with_byte_size(1 << 20, 0);

#[repr(C)]
struct PacketLog {
    source: [u8; 4],
    destination: [u8; 4],
    eth_type: u16,
    decision: u8,
    _padding: u8,
    ingress_ifindex: u32,
}

#[panic_handler]
fn panic(_info: &PanicInfo) -> ! {
    unsafe { core::hint::unreachable_unchecked() }
}

#[xdp]
pub fn traffic_drop(ctx: XdpContext) -> u32 {
    try_traffic_drop(&ctx)
}

fn try_traffic_drop(ctx: &XdpContext) -> u32 {
    let eth_type = match read_u16_be(ctx, ETH_TYPE_OFFSET) {
        Ok(value) => value,
        Err(_) => {
            log_packet(ctx, [0; 4], [0; 4], 0, DECISION_DROP);
            return xdp_action::XDP_DROP;
        }
    };

    match eth_type {
        ETH_P_IP => handle_ipv4(ctx, eth_type),
        ETH_P_ARP => handle_arp(ctx, eth_type),
        _ => {
            log_packet(ctx, [0; 4], [0; 4], eth_type, DECISION_PASS);
            xdp_action::XDP_PASS
        }
    }
}

fn handle_ipv4(ctx: &XdpContext, eth_type: u16) -> u32 {
    let source = match read_ipv4(ctx, IPV4_SRC_OFFSET) {
        Ok(value) => value,
        Err(_) => {
            log_packet(ctx, [0; 4], [0; 4], eth_type, DECISION_DROP);
            return xdp_action::XDP_DROP;
        }
    };
    let destination = match read_ipv4(ctx, IPV4_DST_OFFSET) {
        Ok(value) => value,
        Err(_) => {
            log_packet(ctx, source, [0; 4], eth_type, DECISION_DROP);
            return xdp_action::XDP_DROP;
        }
    };

    let source_allowed = unsafe { ALLOWED_SOURCES.get(&source).is_some() };
    let destination_allowed = unsafe { ALLOWED_SOURCES.get(&destination).is_some() };
    if source_allowed && destination_allowed {
        log_packet(ctx, source, destination, eth_type, DECISION_PASS);
        return xdp_action::XDP_PASS;
    }

    log_packet(ctx, source, destination, eth_type, DECISION_DROP);
    xdp_action::XDP_DROP
}

fn handle_arp(ctx: &XdpContext, eth_type: u16) -> u32 {
    if !is_ethernet_ipv4_arp(ctx) {
        log_packet(ctx, [0; 4], [0; 4], eth_type, DECISION_DROP);
        return xdp_action::XDP_DROP;
    }

    let source = match read_ipv4(ctx, ARP_SPA_OFFSET) {
        Ok(value) => value,
        Err(_) => {
            log_packet(ctx, [0; 4], [0; 4], eth_type, DECISION_DROP);
            return xdp_action::XDP_DROP;
        }
    };
    let destination = match read_ipv4(ctx, ARP_TPA_OFFSET) {
        Ok(value) => value,
        Err(_) => {
            log_packet(ctx, source, [0; 4], eth_type, DECISION_DROP);
            return xdp_action::XDP_DROP;
        }
    };

    let source_allowed = unsafe { ALLOWED_SOURCES.get(&source).is_some() };
    let destination_allowed = unsafe { ALLOWED_SOURCES.get(&destination).is_some() };
    if source_allowed && destination_allowed {
        log_packet(ctx, source, destination, eth_type, DECISION_PASS);
        return xdp_action::XDP_PASS;
    }

    log_packet(ctx, source, destination, eth_type, DECISION_DROP);
    xdp_action::XDP_DROP
}

fn is_ethernet_ipv4_arp(ctx: &XdpContext) -> bool {
    let htype = match read_u16_be(ctx, ARP_HTYPE_OFFSET) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let ptype = match read_u16_be(ctx, ARP_PTYPE_OFFSET) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let hlen = match read_u8(ctx, ARP_HLEN_OFFSET) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let plen = match read_u8(ctx, ARP_PLEN_OFFSET) {
        Ok(value) => value,
        Err(_) => return false,
    };

    if htype != ARP_HTYPE_ETHERNET
        || ptype != ETH_P_IP
        || hlen != ARP_HLEN_ETHERNET
        || plen != ARP_PLEN_IPV4
    {
        return false;
    }

    packet_has_range(
        ctx,
        ARP_SHA_OFFSET,
        ARP_HLEN_ETHERNET as usize
            + ARP_PLEN_IPV4 as usize
            + ARP_HLEN_ETHERNET as usize
            + ARP_PLEN_IPV4 as usize,
    )
}

fn read_u8(ctx: &XdpContext, offset: usize) -> Result<u8, ()> {
    let data = ctx.data();
    let data_end = ctx.data_end();

    if data + offset + 1 > data_end {
        return Err(());
    }

    let ptr = (data + offset) as *const u8;
    Ok(unsafe { *ptr })
}

fn read_u16_be(ctx: &XdpContext, offset: usize) -> Result<u16, ()> {
    let data = ctx.data();
    let data_end = ctx.data_end();

    if data + offset + 2 > data_end {
        return Err(());
    }

    let ptr = (data + offset) as *const u8;
    Ok(u16::from_be_bytes(unsafe { [*ptr, *ptr.add(1)] }))
}

fn read_ipv4(ctx: &XdpContext, offset: usize) -> Result<[u8; 4], ()> {
    let data = ctx.data();
    let data_end = ctx.data_end();

    if data + offset + IPV4_SRC_LEN > data_end {
        return Err(());
    }

    let ptr = (data + offset) as *const u8;
    Ok(unsafe { [*ptr, *ptr.add(1), *ptr.add(2), *ptr.add(3)] })
}

fn packet_has_range(ctx: &XdpContext, offset: usize, len: usize) -> bool {
    let data = ctx.data();
    let data_end = ctx.data_end();

    data + offset + len <= data_end
}

fn log_packet(
    ctx: &XdpContext,
    source: [u8; 4],
    destination: [u8; 4],
    eth_type: u16,
    decision: u8,
) {
    let event = PacketLog {
        source,
        destination,
        eth_type,
        decision,
        _padding: 0,
        ingress_ifindex: unsafe { (*ctx.ctx).ingress_ifindex },
    };

    let _ = PACKET_LOGS.output(&event, 0);
}
