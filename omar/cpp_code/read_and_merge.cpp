#include <iostream>
#include <cassert>
#include <sstream>
#include <map>
#include <vector>
#include <string>
#include <fcntl.h>
#include <stdint.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/mman.h>
#include <algorithm>
#include "read_and_merge.h"


double calculate_average_urgent_std(const std::vector<SegmentStdStat>& segment_traffic_std, UrgentStdType type) {
    double sum = 0.0;
    for (const auto& stat : segment_traffic_std) {
        if (type == UrgentStdType::Write){
            sum += stat.write_urgent_std;
        } 
        else if (type == UrgentStdType::Read){
            sum += stat.read_urgent_std;
        } 
        else if (type == UrgentStdType::All){
            sum += stat.write_urgent_std + stat.read_urgent_std;
        }
        else{
            std::cerr << "Invalid UrgentStdType: " << static_cast<int>(type) << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    return segment_traffic_std.empty() ? 0.0 : sum / segment_traffic_std.size();
}

bool operator==(const SegmentSummary& lhs, const SegmentSummary& rhs) {
    return lhs.segmentId == rhs.segmentId &&
           lhs.traffic.read_urgent_sum == rhs.traffic.read_urgent_sum &&
           lhs.traffic.write_urgent_sum == rhs.traffic.write_urgent_sum &&
           lhs.traffic.read_instant_sum == rhs.traffic.read_instant_sum &&
           lhs.traffic.write_instant_sum == rhs.traffic.write_instant_sum &&
           lhs.traffic.read_longterm_sum == rhs.traffic.read_longterm_sum &&
           lhs.traffic.write_longterm_sum == rhs.traffic.write_longterm_sum;
}

template <typename T>
void AddValues(T& target, uint64_t read_urgent, uint64_t write_urgent, uint64_t read_instant, uint64_t write_instant, uint64_t read_longterm, uint64_t write_longterm) {
    target.read_urgent_sum += read_urgent;
    target.write_urgent_sum += write_urgent;
    target.read_instant_sum += read_instant;
    target.write_instant_sum += write_instant;
    target.read_longterm_sum += read_longterm;
    target.write_longterm_sum += write_longterm;
}

bool operator==(const DeviceSummary& lhs, const DeviceSummary& rhs) {
    return lhs.device_id == rhs.device_id &&
           lhs.segment_index == rhs.segment_index &&
           lhs.traffic.read_urgent_sum == rhs.traffic.read_urgent_sum &&
           lhs.traffic.write_urgent_sum == rhs.traffic.write_urgent_sum &&
           lhs.traffic.read_instant_sum == rhs.traffic.read_instant_sum &&
           lhs.traffic.write_instant_sum == rhs.traffic.write_instant_sum &&
           lhs.traffic.read_longterm_sum == rhs.traffic.read_longterm_sum &&
           lhs.traffic.write_longterm_sum == rhs.traffic.write_longterm_sum;
}

std::vector<SegmentShmIoStat> read_segment_iostats_mmap(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "Failed to open file: " << path << std::endl;
        return {};
    }

    struct stat st;
    if (fstat(fd, &st) == -1) {
        close(fd);
        std::cerr << "Failed to fstat file: " << path << std::endl;
        return {};
    }
    // std::cout << "File size: " << st.st_size << std::endl;
    void* mapped = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) {
        close(fd);
        std::cerr << "Failed to mmap file: " << path << std::endl;
        return {};
    }

    ShmStatFileHeader* header = static_cast<ShmStatFileHeader*>(mapped);
    size_t record_size = header->recordSize;
    size_t capacity = 1 << header->capacityBits;
    const SegmentShmIoStat* data_start = reinterpret_cast<const SegmentShmIoStat*>(static_cast<char*>(mapped) + sizeof(*header));

    std::vector<SegmentShmIoStat> results;
    results.reserve(capacity);
    for (size_t i = 0; i < capacity; ++i) {
        const SegmentShmIoStat& e = data_start[i];
        if (e.segmentId.device_id > 0) {
            results.emplace_back(e);
        } else {
            break;
        }
    }
    munmap(mapped, st.st_size);
    close(fd);
    return results;
}

std::string bs_ip_transform(uint64_t bsId){
    uint32_t front_32_bits = bsId >> 32;
    std::stringstream ip_ss;
    ip_ss << (front_32_bits & 0xFF) << '.'
          << ((front_32_bits >> 8) & 0xFF) << '.'
          << ((front_32_bits >> 16) & 0xFF) << '.'
          << ((front_32_bits >> 24) & 0xFF);

    uint16_t mid_16_bits = (bsId >> 16) & (0xFFFF);
    uint16_t port = ((mid_16_bits & 0xFF) << 8) | ((mid_16_bits & 0xFF00) >> 8);
    uint8_t family = bsId & 0xFF;
    std::string ip_address = ip_ss.str();
    std::string ip_port = ip_address + ":" + std::to_string(port);
    return ip_port;
}

std::string bs_ip_transform_cache(uint64_t bsId){
    auto it = bsIdToIp.find(bsId);
    if (it != bsIdToIp.end()){
        return it->second;
    }
    std::string ip_port = bs_ip_transform(bsId);
    bsIdToIp[bsId] = ip_port;
    return ip_port;
}

extern "C" ReturnSegStat merge_bs_segment(int sort_flag) {
    auto iostats = read_segment_iostats_mmap("/var/run/pangu_blockmaster_seg_iostats");
    std::map<std::string, BsSumState> bs_flow;
    BsSegTrafficMap bssegmap;
    for (const auto& e : iostats) {
        std::string bs_ip = bs_ip_transform_cache(e.bsId);
        auto bsIt = bs_flow.find(bs_ip);
        if (bsIt == bs_flow.end()) {
            bs_flow[bs_ip] = BsSumState();
            bsIt = bs_flow.find(bs_ip);
        }
        bsIt->second.AddResult(e.urgent_flow.readBytes, e.urgent_flow.writeBytes, e.instant_flow.readBytes, e.instant_flow.writeBytes, e.longterm_flow.readBytes, e.longterm_flow.writeBytes, e.urgent_latency.readLatency, e.urgent_latency.writeLatency, e.instant_latency.readLatency, e.instant_latency.writeLatency, e.longterm_latency.readLatency, e.longterm_latency.writeLatency, e.urgent_iops.readIops, e.urgent_iops.writeIops, e.instant_iops.readIops, e.instant_iops.writeIops, e.longterm_iops.readIops, e.longterm_iops.writeIops);
        auto& segVec = bssegmap[bs_ip];
        auto seg_traffic_std = SegmentStdStat(e.urgent_flow_std.readStd, e.urgent_flow_std.writeStd, e.instant_flow_std.readStd, e.instant_flow_std.writeStd, e.longterm_flow_std.readStd, e.longterm_flow_std.writeStd);
        auto seg_traffic = SumTraffic(e.urgent_flow.readBytes, e.urgent_flow.writeBytes, e.instant_flow.readBytes, e.instant_flow.writeBytes, e.longterm_flow.readBytes, e.longterm_flow.writeBytes);
        auto seg_latency = SumLatency(e.urgent_latency.readLatency, e.urgent_latency.writeLatency, e.instant_latency.readLatency, e.instant_latency.writeLatency, e.longterm_latency.readLatency, e.longterm_latency.writeLatency);
        auto seg_iops = SumIops(e.urgent_iops.readIops, e.urgent_iops.writeIops, e.instant_iops.readIops, e.instant_iops.writeIops, e.longterm_iops.readIops, e.longterm_iops.writeIops);
        auto segsum = SegmentSummary(e.segmentId, seg_traffic, seg_latency, seg_iops, seg_traffic_std);
        segVec.emplace_back(segsum);
    }
    int16_t maxblastradius = sortBsSegMap(bssegmap, "write", sort_flag);
    double avgblastradius = static_cast<double>(bssegmap.size()) / maxblastradius;
    ReturnSegStat result;
    result.bs_flow = bs_flow;
    result.sortSegMap = bssegmap;
    BlastRadius blastRadius;
    blastRadius.avgblastradius = avgblastradius;
    blastRadius.maxblastradius = maxblastradius;
    result.blastRadius = blastRadius;
    return result;
}

extern "C" std::map<std::string, BsSumState> bs_stat() {
    auto iostats = read_segment_iostats_mmap("/var/run/pangu_blockmaster_seg_iostats");
    std::map<std::string, BsSumState> bs_flow;
    for (const auto& e : iostats) {
        std::string bs_ip = bs_ip_transform_cache(e.bsId);
        auto bsIt = bs_flow.find(bs_ip);
        if (bsIt == bs_flow.end()) {
            bs_flow[bs_ip] = BsSumState();
            bsIt = bs_flow.find(bs_ip);
        }
        bsIt->second.AddResult(e.urgent_flow.readBytes, e.urgent_flow.writeBytes, e.instant_flow.readBytes, e.instant_flow.writeBytes, e.longterm_flow.readBytes, e.longterm_flow.writeBytes, e.urgent_latency.readLatency, e.urgent_latency.writeLatency, e.instant_latency.readLatency, e.instant_latency.writeLatency, e.longterm_latency.readLatency, e.longterm_latency.writeLatency, e.urgent_iops.readIops, e.urgent_iops.writeIops, e.instant_iops.readIops, e.instant_iops.writeIops, e.longterm_iops.readIops, e.longterm_iops.writeIops);
    }
    return bs_flow;
}

extern "C" ReturnDevStat merge_bs_device(int sort_flag) {
    auto iostats = read_segment_iostats_mmap("/var/run/pangu_blockmaster_seg_iostats");
    std::map<std::string, BsSumState> bs_flow;
    BsDeviceTrafficMap bsdevicemap;
    for (const auto& e : iostats) {
        std::string bs_ip = bs_ip_transform_cache(e.bsId);
        auto bsIt = bs_flow.find(bs_ip);
        if (bsIt == bs_flow.end()) {
            bs_flow[bs_ip] = BsSumState();
            bsIt = bs_flow.find(bs_ip);
        }
        bsIt->second.AddResult(e.urgent_flow.readBytes, e.urgent_flow.writeBytes, e.instant_flow.readBytes, e.instant_flow.writeBytes, e.longterm_flow.readBytes, e.longterm_flow.writeBytes, e.urgent_latency.readLatency, e.urgent_latency.writeLatency, e.instant_latency.readLatency, e.instant_latency.writeLatency, e.longterm_latency.readLatency, e.longterm_latency.writeLatency, e.urgent_iops.readIops, e.urgent_iops.writeIops, e.instant_iops.readIops, e.instant_iops.writeIops, e.longterm_iops.readIops, e.longterm_iops.writeIops);
        auto& devMap = bsdevicemap[bs_ip];
        auto devIt = devMap.find(e.segmentId.device_id);
        if (devIt == devMap.end()) {
            devMap[e.segmentId.device_id] = DeviceSummary(e.segmentId.device_id);
            devIt = devMap.find(e.segmentId.device_id);
        }
        SegmentStdStat s(e.urgent_flow_std.readStd, e.urgent_flow_std.writeStd, e.instant_flow_std.readStd, e.instant_flow_std.writeStd, e.longterm_flow_std.readStd, e.longterm_flow_std.writeStd);
        devIt->second.AddResult(e.segmentId.segmentIdx, s, e.urgent_flow.readBytes, e.urgent_flow.writeBytes, e.instant_flow.readBytes, e.instant_flow.writeBytes, e.longterm_flow.readBytes, e.longterm_flow.writeBytes, e.urgent_latency.readLatency, e.urgent_latency.writeLatency, e.instant_latency.readLatency, e.instant_latency.writeLatency, e.longterm_latency.readLatency, e.longterm_latency.writeLatency, e.urgent_iops.readIops, e.urgent_iops.writeIops, e.instant_iops.readIops, e.instant_iops.writeIops, e.longterm_iops.readIops, e.longterm_iops.writeIops);
    }
    std::map<std::string, std::vector<DeviceSummary>> sortedBsMap;
    int bs_device_num = 0;
    int16_t maxblastradius = sortBsDevMap(bsdevicemap, sortedBsMap, bs_device_num, "write", sort_flag);
    double avgblastradius = static_cast<double>(bs_device_num) / bsdevicemap.size();
    ReturnDevStat result;
    result.bs_flow = bs_flow;
    result.sortDevMap = sortedBsMap;
    BlastRadius blastRadius;
    blastRadius.avgblastradius = avgblastradius;
    blastRadius.maxblastradius = maxblastradius;
    result.blastRadius = blastRadius;
    return result;
}

extern "C" ReturnRwSegStat merge_bs_rw_segment(int r_sort_flag, int w_sort_flag, double w_traffic, double w_read_traffic_ratio) {
    auto iostats = read_segment_iostats_mmap("/var/run/pangu_blockmaster_seg_iostats");
    std::map<std::string, BsSumState> bs_flow;
    BsSegTrafficMap bssegmap;
    for (const auto& e : iostats) {
        std::string bs_ip = bs_ip_transform_cache(e.bsId);
        auto bsIt = bs_flow.find(bs_ip);
        if (bsIt == bs_flow.end()) {
            bs_flow[bs_ip] = BsSumState();
            bsIt = bs_flow.find(bs_ip);
        }
        bsIt->second.AddResult(e.urgent_flow.readBytes, e.urgent_flow.writeBytes, e.instant_flow.readBytes, e.instant_flow.writeBytes, e.longterm_flow.readBytes, e.longterm_flow.writeBytes, e.urgent_latency.readLatency, e.urgent_latency.writeLatency, e.instant_latency.readLatency, e.instant_latency.writeLatency, e.longterm_latency.readLatency, e.longterm_latency.writeLatency, e.urgent_iops.readIops, e.urgent_iops.writeIops, e.instant_iops.readIops, e.instant_iops.writeIops, e.longterm_iops.readIops, e.longterm_iops.writeIops);
        auto& segVec = bssegmap[bs_ip];
        auto seg_traffic_std = SegmentStdStat(e.urgent_flow_std.readStd, e.urgent_flow_std.writeStd, e.instant_flow_std.readStd, e.instant_flow_std.writeStd, e.longterm_flow_std.readStd, e.longterm_flow_std.writeStd);
        auto seg_traffic = SumTraffic(e.urgent_flow.readBytes, e.urgent_flow.writeBytes, e.instant_flow.readBytes, e.instant_flow.writeBytes, e.longterm_flow.readBytes, e.longterm_flow.writeBytes);
        auto seg_latency = SumLatency(e.urgent_latency.readLatency, e.urgent_latency.writeLatency, e.instant_latency.readLatency, e.instant_latency.writeLatency, e.longterm_latency.readLatency, e.longterm_latency.writeLatency);
        auto seg_iops = SumIops(e.urgent_iops.readIops, e.urgent_iops.writeIops, e.instant_iops.readIops, e.instant_iops.writeIops, e.longterm_iops.readIops, e.longterm_iops.writeIops);
        auto segsum = SegmentSummary(e.segmentId, seg_traffic, seg_latency, seg_iops, seg_traffic_std);
        segVec.emplace_back(segsum);
    }
    ReturnRwSegStat result;
    int16_t maxblastradius = sortBsSegMap(bssegmap, "write", w_sort_flag, w_traffic, w_read_traffic_ratio);
    result.sortWriteSegMap = bssegmap;
    sortBsSegMap(bssegmap, "read", r_sort_flag);
    result.sortReadSegMap = bssegmap;
    double avgblastradius = static_cast<double>(bssegmap.size()) / maxblastradius;
    result.bs_flow = bs_flow;
    BlastRadius blastRadius;
    blastRadius.avgblastradius = avgblastradius;
    blastRadius.maxblastradius = maxblastradius;
    result.blastRadius = blastRadius;
    return result;
}

double calculate_segment_score(const int64_t& urgent_traffic, const double& urgent_std, const int64_t& urgent_latency, const int64_t& urgent_iops, const SortType& sortType){
    double score = 0.0;
    switch (sortType){
        case SortType::TrafficStdScore:
            if (urgent_latency != 0){
                score = (W_TRAFFIC * urgent_traffic - W_STD * urgent_std) * urgent_iops / urgent_latency;
            }
            break;
        case SortType::TrafficScore:
            if (urgent_latency != 0){
                score = urgent_traffic * urgent_iops / urgent_latency;
            }
            break;
        default:
            std::cerr << "Invalid sort type: " << static_cast<int>(sortType) << std::endl;
            exit(EXIT_FAILURE);
    }
    return score;
}

void calculate_bs_score(std::map<std::string, BsSumScoreState>& bs_score_flow, double w1){
    std::vector<double> bs_wtraffic, bs_rtraffic, bs_wlatency_iops, bs_rlatency_iops;
    for (auto& bs : bs_score_flow){ 
        bs_wtraffic.emplace_back(bs.second.mTrafficSum.write_urgent_sum);
        bs_rtraffic.emplace_back(bs.second.mTrafficSum.read_urgent_sum);
        if (bs.second.mIopsSum.write_urgent_sum != 0){
            bs_wlatency_iops.emplace_back(bs.second.mLatencySum.write_urgent_sum / bs.second.mIopsSum.write_urgent_sum);
        }
        else{
            bs_wlatency_iops.emplace_back(0);
        }
        if (bs.second.mIopsSum.read_urgent_sum != 0){
            bs_rlatency_iops.emplace_back(bs.second.mLatencySum.read_urgent_sum / bs.second.mIopsSum.read_urgent_sum);
        }
        else{
            bs_rlatency_iops.emplace_back(0);
        }
    }
    double max_wtraffic = *std::max_element(bs_wtraffic.begin(), bs_wtraffic.end());
    double max_rtraffic = *std::max_element(bs_rtraffic.begin(), bs_rtraffic.end());
    double max_wlatency_iops = *std::max_element(bs_wlatency_iops.begin(), bs_wlatency_iops.end());
    double max_rlatency_iops = *std::max_element(bs_rlatency_iops.begin(), bs_rlatency_iops.end());
    int index = 0;
    for (auto& bs : bs_score_flow){
        if (max_wtraffic != 0){
            bs.second.BsWriteScore = w1 * bs_wtraffic[index] / max_wtraffic + (1-w1) * bs_wlatency_iops[index] / max_wlatency_iops;
        }
        else{
            bs.second.BsWriteScore = 0;
        }
        if (max_rtraffic != 0){
            bs.second.BsReadScore = w1 * bs_rtraffic[index] / max_rtraffic + (1-w1) * bs_rlatency_iops[index] / max_rlatency_iops;
        }
        else{
            bs.second.BsReadScore = 0;
        }
        index++;
    }
}

extern "C" ReturnRwSegScoreStat merge_bsscore_rw_segment(int r_sort_flag, int w_sort_flag, double w1) {
    SortType wsortType = static_cast<SortType>(w_sort_flag);
    SortType rsortType = static_cast<SortType>(r_sort_flag);
    assert ((r_sort_flag == w_sort_flag) && (wsortType == SortType::TrafficScore || wsortType == SortType::TrafficStdScore));
    assert (w1 >= 0.5);
    auto iostats = read_segment_iostats_mmap("/var/run/pangu_blockmaster_seg_iostats");
    std::map<std::string, BsSumScoreState> bs_score_flow;
    BsSegScoreMap bssegmap;
    for (const auto& e : iostats) {
        std::string bs_ip = bs_ip_transform_cache(e.bsId);
        auto bsIt = bs_score_flow.find(bs_ip);
        if (bsIt == bs_score_flow.end()) {
            bs_score_flow[bs_ip] = BsSumScoreState();
            bsIt = bs_score_flow.find(bs_ip);
        }
        bsIt->second.AddResult(e.urgent_flow.readBytes, e.urgent_flow.writeBytes, e.instant_flow.readBytes, e.instant_flow.writeBytes, e.longterm_flow.readBytes, e.longterm_flow.writeBytes, e.urgent_latency.readLatency, e.urgent_latency.writeLatency, e.instant_latency.readLatency, e.instant_latency.writeLatency, e.longterm_latency.readLatency, e.longterm_latency.writeLatency, e.urgent_iops.readIops, e.urgent_iops.writeIops, e.instant_iops.readIops, e.instant_iops.writeIops, e.longterm_iops.readIops, e.longterm_iops.writeIops);
        bsIt->second.AddScore(w_sort_flag, w1, e.urgent_flow.readBytes, e.urgent_flow.writeBytes, e.urgent_flow_std.readStd, e.urgent_flow_std.writeStd, e.urgent_latency.readLatency, e.urgent_latency.writeLatency, e.urgent_iops.readIops, e.urgent_iops.writeIops);
        auto& segVec = bssegmap[bs_ip];
        auto seg_traffic_std = SegmentStdStat(e.urgent_flow_std.readStd, e.urgent_flow_std.writeStd, e.instant_flow_std.readStd, e.instant_flow_std.writeStd, e.longterm_flow_std.readStd, e.longterm_flow_std.writeStd);
        auto seg_traffic = SumTraffic(e.urgent_flow.readBytes, e.urgent_flow.writeBytes, e.instant_flow.readBytes, e.instant_flow.writeBytes, e.longterm_flow.readBytes, e.longterm_flow.writeBytes);
        auto seg_latency = SumLatency(e.urgent_latency.readLatency, e.urgent_latency.writeLatency, e.instant_latency.readLatency, e.instant_latency.writeLatency, e.longterm_latency.readLatency, e.longterm_latency.writeLatency);
        auto seg_iops = SumIops(e.urgent_iops.readIops, e.urgent_iops.writeIops, e.instant_iops.readIops, e.instant_iops.writeIops, e.longterm_iops.readIops, e.longterm_iops.writeIops);
        auto read_score = calculate_segment_score(e.urgent_flow.readBytes, e.urgent_flow_std.readStd, e.urgent_latency.readLatency, e.urgent_iops.readIops, wsortType);
        auto write_score = calculate_segment_score(e.urgent_flow.writeBytes, e.urgent_flow_std.writeStd, e.urgent_latency.writeLatency, e.urgent_iops.writeIops, wsortType);
        auto segsum = SegmentScoreSummary(e.segmentId, seg_traffic, seg_latency, seg_iops, seg_traffic_std, read_score, write_score);
        segVec.emplace_back(segsum);
    }
    // calculate_bs_score(bs_score_flow, w1);
    ReturnRwSegScoreStat result;
    int16_t maxblastradius = sortBsSegScoreMap(bssegmap, "write");
    result.sortWriteSegMap = bssegmap;
    sortBsSegScoreMap(bssegmap, "read");
    result.sortReadSegMap = bssegmap;
    double avgblastradius = static_cast<double>(bssegmap.size()) / maxblastradius;
    result.bs_score_flow = bs_score_flow;
    BlastRadius blastRadius;
    blastRadius.avgblastradius = avgblastradius;
    blastRadius.maxblastradius = maxblastradius;
    result.blastRadius = blastRadius;
    return result;
}

extern "C" ReturnRwDevStat merge_bs_rw_device(int r_sort_flag, int w_sort_flag) {
    auto iostats = read_segment_iostats_mmap("/var/run/pangu_blockmaster_seg_iostats");
    std::map<std::string, BsSumState> bs_flow;
    BsDeviceTrafficMap bsdevicemap;
    for (const auto& e : iostats) {
        std::string bs_ip = bs_ip_transform_cache(e.bsId);
        auto bsIt = bs_flow.find(bs_ip);
        if (bsIt == bs_flow.end()) {
            bs_flow[bs_ip] = BsSumState();
            bsIt = bs_flow.find(bs_ip);
        }
        bsIt->second.AddResult(e.urgent_flow.readBytes, e.urgent_flow.writeBytes, e.instant_flow.readBytes, e.instant_flow.writeBytes, e.longterm_flow.readBytes, e.longterm_flow.writeBytes, e.urgent_latency.readLatency, e.urgent_latency.writeLatency, e.instant_latency.readLatency, e.instant_latency.writeLatency, e.longterm_latency.readLatency, e.longterm_latency.writeLatency, e.urgent_iops.readIops, e.urgent_iops.writeIops, e.instant_iops.readIops, e.instant_iops.writeIops, e.longterm_iops.readIops, e.longterm_iops.writeIops);
        auto& devMap = bsdevicemap[bs_ip];
        auto devIt = devMap.find(e.segmentId.device_id);
        if (devIt == devMap.end()) {
            devMap[e.segmentId.device_id] = DeviceSummary(e.segmentId.device_id);
            devIt = devMap.find(e.segmentId.device_id);
        }
        SegmentStdStat s(e.urgent_flow_std.readStd, e.urgent_flow_std.writeStd, e.instant_flow_std.readStd, e.instant_flow_std.writeStd, e.longterm_flow_std.readStd, e.longterm_flow_std.writeStd);
        devIt->second.AddResult(e.segmentId.segmentIdx, s, e.urgent_flow.readBytes, e.urgent_flow.writeBytes, e.instant_flow.readBytes, e.instant_flow.writeBytes, e.longterm_flow.readBytes, e.longterm_flow.writeBytes, e.urgent_latency.readLatency, e.urgent_latency.writeLatency, e.instant_latency.readLatency, e.instant_latency.writeLatency, e.longterm_latency.readLatency, e.longterm_latency.writeLatency, e.urgent_iops.readIops, e.urgent_iops.writeIops, e.instant_iops.readIops, e.instant_iops.writeIops, e.longterm_iops.readIops, e.longterm_iops.writeIops);
    }
    ReturnRwDevStat result;
    int bs_device_num = 0;
    std::map<std::string, std::vector<DeviceSummary>> sortedWriteBsMap;
    int16_t maxblastradius = sortBsDevMap(bsdevicemap, sortedWriteBsMap, bs_device_num, "write", w_sort_flag);
    double avgblastradius = static_cast<double>(bs_device_num) / bsdevicemap.size();
    result.sortWriteDevMap = sortedWriteBsMap;
    bs_device_num = 0;
    std::map<std::string, std::vector<DeviceSummary>> sortedReadBsMap;
    sortBsDevMap(bsdevicemap, sortedReadBsMap, bs_device_num, "read", r_sort_flag);
    result.sortReadDevMap = sortedReadBsMap;
    result.bs_flow = bs_flow;
    BlastRadius blastRadius;
    blastRadius.avgblastradius = avgblastradius;
    blastRadius.maxblastradius = maxblastradius;
    result.blastRadius = blastRadius;
    return result;
}

int16_t sortBsDevMap(BsDeviceTrafficMap& bsdevicemap, std::map<std::string, std::vector<DeviceSummary>>& sortedBsMap, int& bs_device_num, std::string sort_type, int sort_flag){
    int16_t maxblastradius = 0;
    SortType sortType = static_cast<SortType>(sort_flag);
    for (const auto& bsEntry : bsdevicemap) {
        std::string bs_ip = bsEntry.first;
        const auto& devMap = bsEntry.second;
        bs_device_num += devMap.size();
        maxblastradius = std::max(maxblastradius, static_cast<int16_t>(devMap.size()));
        std::vector<DeviceSummary> devices;
        // devices.reserve(bsEntry.second.size());
        for (const auto& deviceEntry : devMap) {
            devices.emplace_back(deviceEntry.second);
        }
        switch (sortType){
            case SortType::Traffic:
                if (sort_type == "write"){
                    std::sort(devices.begin(), devices.end(), [](const DeviceSummary& a, const DeviceSummary& b) {
                        return a.traffic.write_urgent_sum > b.traffic.write_urgent_sum;
                    });
                }
                else if (sort_type == "read"){
                    std::sort(devices.begin(), devices.end(), [](const DeviceSummary& a, const DeviceSummary& b) {
                        return a.traffic.read_urgent_sum > b.traffic.read_urgent_sum;
                    });
                }
                else{
                    std::cerr << "Invalid sort type: " << sort_type << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            case SortType::TrafficStd:
                if (sort_type == "write"){
                    std::sort(devices.begin(), devices.end(), [](const DeviceSummary& a, const DeviceSummary& b) {
                        double a_std = calculate_average_urgent_std(a.segment_traffic_std, UrgentStdType::Write);
                        double b_std = calculate_average_urgent_std(b.segment_traffic_std, UrgentStdType::Write);
                        return (W_TRAFFIC * a.traffic.write_urgent_sum - W_STD * a_std) > (W_TRAFFIC * b.traffic.write_urgent_sum - W_STD * b_std);
                    });
                }
                else if (sort_type == "read"){
                    std::sort(devices.begin(), devices.end(), [](const DeviceSummary& a, const DeviceSummary& b) {
                        double a_std = calculate_average_urgent_std(a.segment_traffic_std, UrgentStdType::Read);
                        double b_std = calculate_average_urgent_std(b.segment_traffic_std, UrgentStdType::Read);
                        return (W_TRAFFIC * a.traffic.read_urgent_sum - W_STD * a_std) > (W_TRAFFIC * b.traffic.read_urgent_sum - W_STD * b_std);
                    });
                }
                else{
                    std::cerr << "Invalid sort type: " << sort_type << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            case SortType::TrafficIopsLatency:
                if (sort_type == "write"){
                    std::sort(devices.begin(), devices.end(), [](const DeviceSummary& a, const DeviceSummary& b) {
                        double a_score = traffic_weight * a.traffic.write_urgent_sum + iops_weight * a.iops.write_urgent_sum + latency_weight * a.latency.write_urgent_sum - std_weight * calculate_average_urgent_std(a.segment_traffic_std, UrgentStdType::Write);
                        double b_score = traffic_weight * b.traffic.write_urgent_sum + iops_weight * b.iops.write_urgent_sum + latency_weight * b.latency.write_urgent_sum - std_weight * calculate_average_urgent_std(b.segment_traffic_std, UrgentStdType::Write);
                        return a_score > b_score;
                    });
                }
                else if (sort_type == "read"){
                    std::sort(devices.begin(), devices.end(), [](const DeviceSummary& a, const DeviceSummary& b) {
                        double a_score = traffic_weight * a.traffic.read_urgent_sum + iops_weight * a.iops.read_urgent_sum + latency_weight * a.latency.read_urgent_sum - std_weight * calculate_average_urgent_std(a.segment_traffic_std, UrgentStdType::Read);
                        double b_score = traffic_weight * b.traffic.read_urgent_sum + iops_weight * b.iops.read_urgent_sum + latency_weight * b.latency.read_urgent_sum - std_weight * calculate_average_urgent_std(b.segment_traffic_std, UrgentStdType::Read);
                        return a_score > b_score;
                    });
                }
                else{
                    std::cerr << "Invalid sort type: " << sort_type << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            case SortType::wrTrafficStd:
                std::sort(devices.begin(), devices.end(), [](const DeviceSummary& a, const DeviceSummary& b) {
                    uint64_t a_traffic = a.traffic.write_urgent_sum + a.traffic.read_urgent_sum;
                    uint64_t b_traffic = b.traffic.write_urgent_sum + b.traffic.read_urgent_sum;
                    double a_std = calculate_average_urgent_std(a.segment_traffic_std, UrgentStdType::All);
                    double b_std = calculate_average_urgent_std(b.segment_traffic_std, UrgentStdType::All);
                    return (W_TRAFFIC * a_traffic - W_STD * a_std) > (W_TRAFFIC * b_traffic - W_STD * b_std);
                });
                break;
            case SortType::Latency:
                if (sort_type == "write"){
                    std::sort(devices.begin(), devices.end(), [](const DeviceSummary& a, const DeviceSummary& b) {
                        return a.latency.write_urgent_sum > b.latency.write_urgent_sum;
                    });
                }
                else if (sort_type == "read"){
                    std::sort(devices.begin(), devices.end(), [](const DeviceSummary& a, const DeviceSummary& b) {
                        return a.latency.read_urgent_sum > b.latency.read_urgent_sum;
                    });
                }
                else{
                    std::cerr << "Invalid sort type: " << sort_type << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            case SortType::LatencyPerIops:
                if (sort_type == "write"){
                    std::sort(devices.begin(), devices.end(), [](const DeviceSummary& a, const DeviceSummary& b) {
                        if (a.iops.write_urgent_sum == 0){
                            return false;
                        }
                        if (b.iops.write_urgent_sum == 0){
                            return true;
                        }
                        return a.latency.write_urgent_sum / a.iops.write_urgent_sum > b.latency.write_urgent_sum / b.iops.write_urgent_sum;
                    });
                }
                else if (sort_type == "read"){
                    std::sort(devices.begin(), devices.end(), [](const DeviceSummary& a, const DeviceSummary& b) {
                        if (a.iops.read_urgent_sum == 0){
                            return false;
                        }
                        if (b.iops.read_urgent_sum == 0){
                            return true;
                        }
                        return a.latency.read_urgent_sum / a.iops.read_urgent_sum > b.latency.read_urgent_sum / b.iops.read_urgent_sum;
                    });
                }
                else{
                    std::cerr << "Invalid sort type: " << sort_type << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            default:
                std::cerr << "Invalid sort flag: " << sort_flag << std::endl;
                exit(EXIT_FAILURE);
        }
        sortedBsMap[bs_ip] = std::move(devices);
    }
    return maxblastradius;
}

int16_t sortBsSegMap(BsSegTrafficMap& bssegmap, std::string sort_type, int sort_flag, double w_traffic, double w_read_traffic_ratio){
    int16_t maxblastradius = 0;
    SortType sortType = static_cast<SortType>(sort_flag);
    for(auto& bsEntry : bssegmap){
        std::string bs_ip = bsEntry.first;
        auto& segVec = bsEntry.second;
        maxblastradius = std::max(maxblastradius, static_cast<int16_t>(segVec.size()));
        switch (sortType)
        {
            case SortType::Traffic:
                if (sort_type == "write"){
                    std::sort(segVec.begin(), segVec.end(), [](const SegmentSummary& a, const SegmentSummary& b){
                        return a.traffic.write_urgent_sum > b.traffic.write_urgent_sum;
                    });
                }
                else if (sort_type == "read"){
                    std::sort(segVec.begin(), segVec.end(), [](const SegmentSummary& a, const SegmentSummary& b){
                        return a.traffic.read_urgent_sum > b.traffic.read_urgent_sum;
                    });
                }
                else{
                    std::cerr << "Invalid sort type: " << sort_type << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            case SortType::TrafficStd:
                if (sort_type == "write"){
                    std::sort(segVec.begin(), segVec.end(), [&w_traffic](const SegmentSummary& a, const SegmentSummary& b){
                        return (w_traffic * a.traffic.write_urgent_sum - (1-w_traffic) * a.traffic_std.write_urgent_std) > (w_traffic * b.traffic.write_urgent_sum - (1-w_traffic) * b.traffic_std.write_urgent_std);
                    });
                }
                else if (sort_type == "read"){
                    std::sort(segVec.begin(), segVec.end(), [&w_traffic](const SegmentSummary& a, const SegmentSummary& b){
                        return (w_traffic * a.traffic.read_urgent_sum - (1-w_traffic) * a.traffic_std.read_urgent_std) > (w_traffic * b.traffic.read_urgent_sum - (1-w_traffic) * b.traffic_std.read_urgent_std);
                    });
                }
                else{
                    std::cerr << "Invalid sort type: " << sort_type << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            case SortType::TrafficIopsLatency:
                if (sort_type == "write"){
                    std::sort(segVec.begin(), segVec.end(), [](const SegmentSummary& a, const SegmentSummary& b){
                        return (traffic_weight * a.traffic.write_urgent_sum + iops_weight * a.iops.write_urgent_sum + latency_weight * a.latency.write_urgent_sum - std_weight * a.traffic_std.write_urgent_std ) > (traffic_weight * b.traffic.write_urgent_sum + iops_weight * b.iops.write_urgent_sum + latency_weight * b.latency.write_urgent_sum - std_weight * b.traffic_std.write_urgent_std);
                    });
                }
                else if (sort_type == "read"){
                    std::sort(segVec.begin(), segVec.end(), [](const SegmentSummary& a, const SegmentSummary& b){
                        return (traffic_weight * a.traffic.read_urgent_sum + iops_weight * a.iops.read_urgent_sum + latency_weight * a.latency.read_urgent_sum - std_weight * a.traffic_std.read_urgent_std ) > (traffic_weight * b.traffic.read_urgent_sum + iops_weight * b.iops.read_urgent_sum + latency_weight * b.latency.read_urgent_sum - std_weight * b.traffic_std.read_urgent_std);
                    });
                }
                else{
                    std::cerr << "Invalid sort type: " << sort_type << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            case SortType::wrTrafficStd:
                std::sort(segVec.begin(), segVec.end(), [](const SegmentSummary& a, const SegmentSummary& b){
                    uint64_t a_traffic = a.traffic.write_urgent_sum + a.traffic.read_urgent_sum;
                    double a_std = a.traffic_std.write_urgent_std + a.traffic_std.read_urgent_std;
                    uint64_t b_traffic = b.traffic.write_urgent_sum + b.traffic.read_urgent_sum;
                    double b_std = b.traffic_std.write_urgent_std + b.traffic_std.read_urgent_std;
                    return (W_TRAFFIC * a_traffic - W_STD * a_std) > (W_TRAFFIC * b_traffic - W_STD * b_std);
                });
                break;
            case SortType::Latency:
                if (sort_type == "write"){
                    std::sort(segVec.begin(), segVec.end(), [](const SegmentSummary& a, const SegmentSummary& b){
                        return a.latency.write_urgent_sum > b.latency.write_urgent_sum;
                    });
                }
                else if (sort_type == "read"){
                    std::sort(segVec.begin(), segVec.end(), [](const SegmentSummary& a, const SegmentSummary& b){
                        return a.latency.read_urgent_sum > b.latency.read_urgent_sum;
                    });
                }
                else{
                    std::cerr << "Invalid sort type: " << sort_type << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            case SortType::LatencyPerIops:
                if (sort_type == "write"){
                    std::sort(segVec.begin(), segVec.end(), [](const SegmentSummary& a, const SegmentSummary& b){
                        if (a.iops.write_urgent_sum == 0){
                            return false;
                        }
                        if (b.iops.write_urgent_sum == 0){
                            return true;
                        }
                        return a.latency.write_urgent_sum / a.iops.write_urgent_sum > b.latency.write_urgent_sum / b.iops.write_urgent_sum;
                    });
                }
                else if (sort_type == "read"){
                    std::sort(segVec.begin(), segVec.end(), [](const SegmentSummary& a, const SegmentSummary& b){
                        if (a.iops.read_urgent_sum == 0){
                            return false;
                        }
                        if (b.iops.read_urgent_sum == 0){
                            return true;
                        }
                        return a.latency.read_urgent_sum / a.iops.read_urgent_sum > b.latency.read_urgent_sum / b.iops.read_urgent_sum;
                    });
                }
                else{
                    std::cerr << "Invalid sort type: " << sort_type << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            case SortType::TrafficStdLong:
                if (sort_type == "write"){
                    std::sort(segVec.begin(), segVec.end(), [](const SegmentSummary& a, const SegmentSummary& b){
                        return (W_TRAFFIC_URGENT * a.traffic.write_urgent_sum - W_STD_URGENT * a.traffic_std.write_urgent_std - W_STD_INSTANT * a.traffic_std.write_instant_std) > (W_TRAFFIC_URGENT * b.traffic.write_urgent_sum - W_STD_URGENT * b.traffic_std.write_urgent_std - W_STD_INSTANT * b.traffic_std.write_instant_std);
                    });
                }
                else if (sort_type == "read"){
                    std::sort(segVec.begin(), segVec.end(), [](const SegmentSummary& a, const SegmentSummary& b){
                        return (W_TRAFFIC_URGENT * a.traffic.read_urgent_sum - W_STD_URGENT * a.traffic_std.read_urgent_std - W_STD_INSTANT * a.traffic_std.read_instant_std) > (W_TRAFFIC_URGENT * b.traffic.read_urgent_sum - W_STD_URGENT * b.traffic_std.read_urgent_std - W_STD_INSTANT * b.traffic_std.read_instant_std);
                    });
                }
                else{
                    std::cerr << "Invalid sort type: " << sort_type << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            case SortType::TrafficStdScore:
                if (sort_type == "write"){
                    std::sort(segVec.begin(), segVec.end(), [&w_traffic](const SegmentSummary& a, const SegmentSummary& b){
                        if (a.latency.write_urgent_sum == 0){
                            return false;
                        }
                        if (b.latency.write_urgent_sum == 0){
                            return true;
                        }
                        double a_score = (w_traffic * a.traffic.write_urgent_sum - (1-w_traffic) * a.traffic_std.write_urgent_std) * a.iops.write_urgent_sum / a.latency.write_urgent_sum;
                        double b_score = (w_traffic * b.traffic.write_urgent_sum - (1-w_traffic) * b.traffic_std.write_urgent_std) * b.iops.write_urgent_sum / b.latency.write_urgent_sum;
                        return a_score > b_score;
                    });
                }
                else if (sort_type == "read"){
                    std::sort(segVec.begin(), segVec.end(), [&w_traffic](const SegmentSummary& a, const SegmentSummary& b){
                        if (a.latency.read_urgent_sum == 0){
                            return false;
                        }
                        if (b.latency.read_urgent_sum == 0){
                            return true;
                        }
                        double a_score = (w_traffic * a.traffic.read_urgent_sum - (1-w_traffic) * a.traffic_std.read_urgent_std) * a.iops.read_urgent_sum / a.latency.read_urgent_sum;
                        double b_score = (w_traffic * b.traffic.read_urgent_sum - (1-w_traffic) * b.traffic_std.read_urgent_std) * b.iops.read_urgent_sum / b.latency.read_urgent_sum;
                        return a_score > b_score;
                    });
                }
                else{
                    std::cerr << "Invalid sort type: " << sort_type << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            case SortType::TrafficStdLatScore:
                if (sort_type == "write"){
                    std::sort(segVec.begin(), segVec.end(), [&w_traffic](const SegmentSummary& a, const SegmentSummary& b){
                        if (a.latency.write_urgent_sum == 0){
                            return false;
                        }
                        if (b.latency.write_urgent_sum == 0){
                            return true;
                        }
                        double a_score = (w_traffic * a.traffic.write_urgent_sum - (1-w_traffic) * a.traffic_std.write_urgent_std) * a.latency.write_urgent_sum;
                        double b_score = (w_traffic * b.traffic.write_urgent_sum - (1-w_traffic) * b.traffic_std.write_urgent_std) * b.latency.write_urgent_sum;
                        return a_score > b_score;
                    });
                }
                else if (sort_type == "read"){
                    std::sort(segVec.begin(), segVec.end(), [&w_traffic](const SegmentSummary& a, const SegmentSummary& b){
                        if (a.latency.read_urgent_sum == 0){
                            return false;
                        }
                        if (b.latency.read_urgent_sum == 0){
                            return true;
                        }
                        double a_score = (w_traffic * a.traffic.read_urgent_sum - (1-w_traffic) * a.traffic_std.read_urgent_std) * a.latency.read_urgent_sum;
                        double b_score = (w_traffic * b.traffic.read_urgent_sum - (1-w_traffic) * b.traffic_std.read_urgent_std) * b.latency.read_urgent_sum;
                        return a_score > b_score;
                    });
                }
                else{
                    std::cerr << "Invalid sort type: " << sort_type << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            case SortType::TrafficStdIopsScore:
                if (sort_type == "write"){
                    std::sort(segVec.begin(), segVec.end(), [&w_traffic](const SegmentSummary& a, const SegmentSummary& b){
                        if (a.latency.write_urgent_sum == 0){
                            return false;
                        }
                        if (b.latency.write_urgent_sum == 0){
                            return true;
                        }
                        double a_score = (w_traffic * a.traffic.write_urgent_sum - (1-w_traffic) * a.traffic_std.write_urgent_std) / a.iops.write_urgent_sum;
                        double b_score = (w_traffic * b.traffic.write_urgent_sum - (1-w_traffic) * b.traffic_std.write_urgent_std) / b.iops.write_urgent_sum;
                        return a_score > b_score;
                    });
                }
                else if (sort_type == "read"){
                    std::sort(segVec.begin(), segVec.end(), [&w_traffic](const SegmentSummary& a, const SegmentSummary& b){
                        if (a.latency.read_urgent_sum == 0){
                            return false;
                        }
                        if (b.latency.read_urgent_sum == 0){
                            return true;
                        }
                        double a_score = (w_traffic * a.traffic.read_urgent_sum - (1-w_traffic) * a.traffic_std.read_urgent_std) / a.iops.read_urgent_sum;
                        double b_score = (w_traffic * b.traffic.read_urgent_sum - (1-w_traffic) * b.traffic_std.read_urgent_std) / b.iops.read_urgent_sum;
                        return a_score > b_score;
                    });
                }
                else{
                    std::cerr << "Invalid sort type: " << sort_type << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            case SortType::TrafficScore:
                if (sort_type == "write"){
                    std::sort(segVec.begin(), segVec.end(), [](const SegmentSummary& a, const SegmentSummary& b){
                        if (a.latency.write_urgent_sum == 0){
                            return false;
                        }
                        if (b.latency.write_urgent_sum == 0){
                            return true;
                        }
                        double a_score = a.traffic.write_urgent_sum * a.iops.write_urgent_sum / a.latency.write_urgent_sum;
                        double b_score = b.traffic.write_urgent_sum * b.iops.write_urgent_sum / b.latency.write_urgent_sum;
                        return a_score > b_score;
                    });
                }
                else if (sort_type == "read"){
                    std::sort(segVec.begin(), segVec.end(), [](const SegmentSummary& a, const SegmentSummary& b){
                        if (a.latency.read_urgent_sum == 0){
                            return false;
                        }
                        if (b.latency.read_urgent_sum == 0){
                            return true;
                        }
                        double a_score = a.traffic.read_urgent_sum * a.iops.read_urgent_sum / a.latency.read_urgent_sum;
                        double b_score = b.traffic.read_urgent_sum * b.iops.read_urgent_sum / b.latency.read_urgent_sum;
                        return a_score > b_score;
                    });
                }
                else{
                    std::cerr << "Invalid sort type: " << sort_type << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            case SortType::ReadRatio:
                assert(sort_type == "read");
                std::sort(segVec.begin(), segVec.end(), [&w_read_traffic_ratio](const SegmentSummary& a, const SegmentSummary& b){
                    double a_score = w_read_traffic_ratio * a.traffic.read_urgent_sum - (1-w_read_traffic_ratio) * a.traffic.write_urgent_sum;
                    double b_score = w_read_traffic_ratio * b.traffic.read_urgent_sum - (1-w_read_traffic_ratio) * b.traffic.write_urgent_sum;
                    return a_score > b_score;
                });
                break;
            default:
                std::cerr << "Invalid sort flag: " << sort_flag << std::endl;
                exit(EXIT_FAILURE);
        }
    }
    return maxblastradius;
}

int16_t sortBsSegScoreMap(BsSegScoreMap& bssegmap, std::string sort_type){
    int16_t maxblastradius = 0;
    for(auto& bsEntry : bssegmap){
        std::string bs_ip = bsEntry.first;
        auto& segVec = bsEntry.second;
        maxblastradius = std::max(maxblastradius, static_cast<int16_t>(segVec.size()));
        if (sort_type == "write"){
            std::sort(segVec.begin(), segVec.end(), [](const SegmentScoreSummary& a, const SegmentScoreSummary& b){
                return a.write_score > b.write_score;
            });
        }
        else if (sort_type == "read"){
            std::sort(segVec.begin(), segVec.end(), [](const SegmentScoreSummary& a, const SegmentScoreSummary& b){
                return a.read_score > b.read_score;
            });
        }
        else{
            std::cerr << "Invalid sort type: " << sort_type << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    return maxblastradius;
}

#if not IF_PYBIND11
#include <chrono>
int main() {
    while(1){
        auto start = std::chrono::high_resolution_clock::now();
        auto return_msg = merge_bs_segment();
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed = end - start;
        std::cout << std::fixed << std::setprecision(6) << "Total execution time: " << elapsed.count() << " seconds" << std::endl;
    }
}

#else
#include <pybind11/pybind11.h>
#include <pybind11/stl_bind.h>
#include <pybind11/stl.h>
namespace py = pybind11;

PYBIND11_MODULE(read_and_merge, m) {
    py::class_<SumTraffic>(m, "SumTraffic")
        .def(py::init<>())
        .def_readwrite("read_urgent_sum", &SumTraffic::read_urgent_sum)
        .def_readwrite("write_urgent_sum", &SumTraffic::write_urgent_sum)
        .def_readwrite("read_instant_sum", &SumTraffic::read_instant_sum)
        .def_readwrite("write_instant_sum", &SumTraffic::write_instant_sum)
        .def_readwrite("read_longterm_sum", &SumTraffic::read_longterm_sum)
        .def_readwrite("write_longterm_sum", &SumTraffic::write_longterm_sum);
    py::class_<SumLatency>(m, "SumLatency")
        .def(py::init<>())
        .def_readwrite("read_urgent_sum", &SumLatency::read_urgent_sum)
        .def_readwrite("write_urgent_sum", &SumLatency::write_urgent_sum)
        .def_readwrite("read_instant_sum", &SumLatency::read_instant_sum)
        .def_readwrite("write_instant_sum", &SumLatency::write_instant_sum)
        .def_readwrite("read_longterm_sum", &SumLatency::read_longterm_sum)
        .def_readwrite("write_longterm_sum", &SumLatency::write_longterm_sum);
    py::class_<SumIops>(m, "SumIops")
        .def(py::init<>())
        .def_readwrite("read_urgent_sum", &SumIops::read_urgent_sum)
        .def_readwrite("write_urgent_sum", &SumIops::write_urgent_sum)
        .def_readwrite("read_instant_sum", &SumIops::read_instant_sum)
        .def_readwrite("write_instant_sum", &SumIops::write_instant_sum)
        .def_readwrite("read_longterm_sum", &SumIops::read_longterm_sum)
        .def_readwrite("write_longterm_sum", &SumIops::write_longterm_sum);
    py::class_<SegmentStdStat>(m, "SegmentStdStat")
        .def(py::init<>())
        .def_readwrite("read_urgent_std", &SegmentStdStat::read_urgent_std)
        .def_readwrite("write_urgent_std", &SegmentStdStat::write_urgent_std)
        .def_readwrite("read_instant_std", &SegmentStdStat::read_instant_std)
        .def_readwrite("write_instant_std", &SegmentStdStat::write_instant_std)
        .def_readwrite("read_longterm_std", &SegmentStdStat::read_longterm_std)
        .def_readwrite("write_longterm_std", &SegmentStdStat::write_longterm_std);

    pybind11::class_<SegmentId>(m, "SegmentId")
        .def(pybind11::init<>())
        .def(pybind11::init<uint64_t, uint32_t, uint32_t>())
        .def_readwrite("device_id", &SegmentId::device_id)
        .def_readwrite("segment_index", &SegmentId::segmentIdx)
        .def_readwrite("padding", &SegmentId::padding)
        .def("__eq__", &SegmentId::operator==);

    py::class_<SegmentSummary>(m, "SegmentSummary")
        .def(py::init<>())
        .def_readwrite("segment_id", &SegmentSummary::segmentId)
        .def_readwrite("traffic", &SegmentSummary::traffic)
        .def_readwrite("latency", &SegmentSummary::latency)
        .def_readwrite("iops", &SegmentSummary::iops)
        .def_readwrite("traffic_std", &SegmentSummary::traffic_std);
    
    py::class_<SegmentScoreSummary>(m, "SegmentScoreSummary")
        .def(py::init<>())
        .def_readwrite("segment_id", &SegmentScoreSummary::segmentId)
        .def_readwrite("traffic", &SegmentScoreSummary::traffic)
        .def_readwrite("latency", &SegmentScoreSummary::latency)
        .def_readwrite("iops", &SegmentScoreSummary::iops)
        .def_readwrite("traffic_std", &SegmentScoreSummary::traffic_std)
        .def_readwrite("write_score", &SegmentScoreSummary::write_score)
        .def_readwrite("read_score", &SegmentScoreSummary::read_score);

    py::class_<DeviceSummary>(m, "DeviceSummary")
        .def(py::init<>())
        .def_readwrite("device_id", &DeviceSummary::device_id)
        .def_readwrite("segment_index", &DeviceSummary::segment_index)
        .def_readwrite("segment_traffic_std", &DeviceSummary::segment_traffic_std)
        .def_readwrite("traffic", &DeviceSummary::traffic)
        .def_readwrite("latency", &DeviceSummary::latency)
        .def_readwrite("iops", &DeviceSummary::iops);

    py::class_<BsSumState>(m, "BsSumState")
        .def(py::init<>())
        .def_readwrite("mTrafficSum", &BsSumState::mTrafficSum)
        .def_readwrite("mLatencySum", &BsSumState::mLatencySum)
        .def_readwrite("mIopsSum", &BsSumState::mIopsSum);

    py::class_<BsSumScoreState>(m, "BsSumScoreState")
        .def(py::init<>())
        .def_readwrite("mTrafficSum", &BsSumScoreState::mTrafficSum)
        .def_readwrite("mLatencySum", &BsSumScoreState::mLatencySum)
        .def_readwrite("mIopsSum", &BsSumScoreState::mIopsSum)
        .def_readwrite("BsReadScore", &BsSumScoreState::BsReadScore)
        .def_readwrite("BsWriteScore", &BsSumScoreState::BsWriteScore);

    py::bind_map<std::map<uint64_t, std::vector<uint64_t>>>(m, "Uint64VectorMap");
    py::bind_vector<std::vector<uint64_t>>(m, "Uint64Vector");
    py::bind_map<std::map<uint64_t, DeviceSummary>>(m, "DeviceSummaryMap");
    py::bind_vector<std::vector<SegmentSummary>>(m, "SegSumVector");

    py::class_<BlastRadius>(m, "BlastRadius")
        .def(py::init<>())
        .def_readwrite("avg_br", &BlastRadius::avgblastradius)
        .def_readwrite("max_br", &BlastRadius::maxblastradius);

    py::class_<ReturnSegStat>(m, "ReturnSegStat")
        .def(py::init<>())
        .def_readwrite("bs_flow", &ReturnSegStat::bs_flow)
        .def_readwrite("sort_bs_seg", &ReturnSegStat::sortSegMap)
        .def_readwrite("blast_radius", &ReturnSegStat::blastRadius);

    py::class_<ReturnDevStat>(m, "ReturnDevStat")
        .def(py::init<>())
        .def_readwrite("bs_flow", &ReturnDevStat::bs_flow)
        .def_readwrite("sort_bs_dev", &ReturnDevStat::sortDevMap)
        .def_readwrite("blast_radius", &ReturnDevStat::blastRadius);

    py::class_<ReturnRwSegStat>(m, "ReturnRwSegStat")
        .def(py::init<>())
        .def_readwrite("bs_flow", &ReturnRwSegStat::bs_flow)
        .def_readwrite("sort_write_seg", &ReturnRwSegStat::sortWriteSegMap)
        .def_readwrite("sort_read_seg", &ReturnRwSegStat::sortReadSegMap)
        .def_readwrite("blast_radius", &ReturnRwSegStat::blastRadius);

    py::class_<ReturnRwDevStat>(m, "ReturnRwDevStat")
        .def(py::init<>())
        .def_readwrite("bs_flow", &ReturnRwDevStat::bs_flow)
        .def_readwrite("sort_write_dev", &ReturnRwDevStat::sortWriteDevMap)
        .def_readwrite("sort_read_dev", &ReturnRwDevStat::sortReadDevMap)
        .def_readwrite("blast_radius", &ReturnRwDevStat::blastRadius);

    py::class_<ReturnRwSegScoreStat>(m, "ReturnRwSegScoreStat")
        .def(py::init<>())
        .def_readwrite("bs_score_flow", &ReturnRwSegScoreStat::bs_score_flow)
        .def_readwrite("sort_write_seg", &ReturnRwSegScoreStat::sortWriteSegMap)
        .def_readwrite("sort_read_seg", &ReturnRwSegScoreStat::sortReadSegMap)
        .def_readwrite("blast_radius", &ReturnRwSegScoreStat::blastRadius);

    m.def("merge_bs_device", &merge_bs_device, "A function that merges BS device statistics", pybind11::arg("sort_flag")=0);
    m.def("merge_bs_segment", &merge_bs_segment, "A function that merges BS segment statistics", pybind11::arg("sort_flag")=0);
    m.def("merge_bs_rw_device", &merge_bs_rw_device, "A function that merges BS read/write device statistics", pybind11::arg("r_sort_flag") = 0, pybind11::arg("w_sort_flag") = 0);
    m.def("merge_bs_rw_segment", &merge_bs_rw_segment, "A function that merges BS read/write segment statistics", pybind11::arg("r_sort_flag") = 0, pybind11::arg("w_sort_flag") = 0, pybind11::arg("w_traffic") = W_TRAFFIC, pybind11::arg("w_read_traffic_ratio") = W_READ_TRAFFIC_RATIO);
    m.def("merge_bsscore_rw_segment", &merge_bsscore_rw_segment, "A function that merges BS score, and read/write segment statistics");
    m.def("bs_stat", &bs_stat, "A function that returns BS statistics");
}
#endif
