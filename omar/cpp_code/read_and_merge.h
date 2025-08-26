#ifndef READ_AND_MERGE_H
#define READ_AND_MERGE_H

#include <map>
#include <string>
#include <vector>

#define IF_PYBIND11 1
#define W_TRAFFIC 0.7
#define W_STD (1 - W_TRAFFIC)

#define W_TRAFFIC_URGENT 0.4
#define W_STD_URGENT 0.4
#define W_STD_INSTANT (1 - W_TRAFFIC_URGENT - W_STD_URGENT)

#define W_READ_TRAFFIC_RATIO 0.7

std::map<std::string, float> weight_map = {
    {"traffic", 0.5},
    {"latency", 0.2},
    {"iops", 0.1},
    {"std", -0.2}
};
double traffic_weight = weight_map["traffic"];
double latency_weight = weight_map["latency"];
double iops_weight = weight_map["iops"];
double std_weight = weight_map["std"];

enum class SortType {
    Traffic = 0,
    TrafficStd = 1,
    TrafficIopsLatency = 2,
    wrTrafficStd = 3,
    Latency = 4,
    LatencyPerIops = 5,
    TrafficStdLong = 6,
    TrafficStdScore = 7,
    TrafficScore = 8,
    ReadRatio = 9,
    TrafficStdLatScore = 10,
    TrafficStdIopsScore = 11,
};

enum class UrgentStdType {
    Read,
    Write,
    All,
};

struct ShmStatFileHeader
{
    uint64_t    magic;
    uint8_t     recordSize;
    uint8_t     recordSizeBits;
    uint8_t     capacityBits;
    char        padding[46];
};

struct SegmentId {
    uint64_t device_id;
    uint32_t segmentIdx;
    uint32_t   padding;

    bool operator==(const SegmentId &other) const {
        return device_id == other.device_id && segmentIdx == other.segmentIdx;
    }
};

struct FlowStat {
    int64_t writeBytes;
    int64_t readBytes;
};

struct FlowStdStat{
    double writeStd;
    double readStd;
};

struct IopsStat{
    int64_t writeIops;
    int64_t readIops;
};

struct LatencyStat{
    int64_t writeLatency;
    int64_t readLatency;
};

struct SegmentShmIoStat
{
    SegmentId       segmentId;
    uint64_t        loadVersion;
    uint64_t        bsId;
    LatencyStat     urgent_latency;
    LatencyStat     instant_latency;
    LatencyStat     longterm_latency;
    IopsStat        urgent_iops;
    IopsStat        instant_iops;
    IopsStat        longterm_iops;
    FlowStat        urgent_flow;
    FlowStat        instant_flow;
    FlowStat        longterm_flow;
    FlowStdStat     urgent_flow_std;
    FlowStdStat     instant_flow_std;
    FlowStdStat     longterm_flow_std;
    SegmentShmIoStat() : loadVersion(0), bsId(0) {}
};

struct SumTraffic{
    uint64_t read_urgent_sum;
    uint64_t write_urgent_sum;
    uint64_t read_instant_sum;
    uint64_t write_instant_sum;
    uint64_t read_longterm_sum;
    uint64_t write_longterm_sum;
    SumTraffic(): read_urgent_sum(0), write_urgent_sum(0), read_instant_sum(0), write_instant_sum(0), read_longterm_sum(0), write_longterm_sum(0) {}
    SumTraffic(uint64_t read_urgent_sum, uint64_t write_urgent_sum, uint64_t read_instant_sum, uint64_t write_instant_sum, uint64_t read_longterm_sum, uint64_t write_longterm_sum) : read_urgent_sum(read_urgent_sum), write_urgent_sum(write_urgent_sum), read_instant_sum(read_instant_sum), write_instant_sum(write_instant_sum), read_longterm_sum(read_longterm_sum), write_longterm_sum(write_longterm_sum) {}
};

struct SumLatency{
    uint64_t read_urgent_sum;
    uint64_t write_urgent_sum;
    uint64_t read_instant_sum;
    uint64_t write_instant_sum;
    uint64_t read_longterm_sum;
    uint64_t write_longterm_sum;
    SumLatency(): read_urgent_sum(0), write_urgent_sum(0), read_instant_sum(0), write_instant_sum(0), read_longterm_sum(0), write_longterm_sum(0) {}
    SumLatency(uint64_t read_urgent_sum, uint64_t write_urgent_sum, uint64_t read_instant_sum, uint64_t write_instant_sum, uint64_t read_longterm_sum, uint64_t write_longterm_sum) : read_urgent_sum(read_urgent_sum), write_urgent_sum(write_urgent_sum), read_instant_sum(read_instant_sum), write_instant_sum(write_instant_sum), read_longterm_sum(read_longterm_sum), write_longterm_sum(write_longterm_sum) {}
};

struct SumIops{
    uint64_t read_urgent_sum;
    uint64_t write_urgent_sum;
    uint64_t read_instant_sum;
    uint64_t write_instant_sum;
    uint64_t read_longterm_sum;
    uint64_t write_longterm_sum;
    SumIops(): read_urgent_sum(0), write_urgent_sum(0), read_instant_sum(0), write_instant_sum(0), read_longterm_sum(0), write_longterm_sum(0) {}
    SumIops(uint64_t read_urgent_sum, uint64_t write_urgent_sum, uint64_t read_instant_sum, uint64_t write_instant_sum, uint64_t read_longterm_sum, uint64_t write_longterm_sum) : read_urgent_sum(read_urgent_sum), write_urgent_sum(write_urgent_sum), read_instant_sum(read_instant_sum), write_instant_sum(write_instant_sum), read_longterm_sum(read_longterm_sum), write_longterm_sum(write_longterm_sum) {}
};

struct SegmentStdStat{
    double read_urgent_std;
    double write_urgent_std;
    double read_instant_std;
    double write_instant_std;
    double read_longterm_std;
    double write_longterm_std;
    SegmentStdStat() : read_urgent_std(0), write_urgent_std(0), read_instant_std(0), write_instant_std(0), read_longterm_std(0), write_longterm_std(0) {}
    SegmentStdStat(double read_urgent_std, double write_urgent_std, double read_instant_std, double write_instant_std, double read_longterm_std, double write_longterm_std) : read_urgent_std(read_urgent_std), write_urgent_std(write_urgent_std), read_instant_std(read_instant_std), write_instant_std(write_instant_std), read_longterm_std(read_longterm_std), write_longterm_std(write_longterm_std) {}
};

double calculate_average_urgent_std(const std::vector<SegmentStdStat>& segment_traffic_std, UrgentStdType type);

struct SegmentSummary{
    SegmentId  segmentId;
    SumTraffic traffic;
    SumLatency latency;
    SumIops    iops;
    SegmentStdStat traffic_std;
    SegmentSummary() = default;
    SegmentSummary(SegmentId segmentId, SumTraffic traffic, SumLatency latency, SumIops iops, SegmentStdStat traffic_std) : segmentId(segmentId), traffic(traffic), latency(latency), iops(iops), traffic_std(traffic_std) {}
};
bool operator==(const SegmentSummary& lhs, const SegmentSummary& rhs);

struct SegmentScoreSummary : public SegmentSummary{
    double read_score;
    double write_score;
    SegmentScoreSummary() : read_score(0.0), write_score(0.0) {}
    SegmentScoreSummary(SegmentId segmentId, SumTraffic traffic, SumLatency latency, SumIops iops, SegmentStdStat traffic_std, double readScore, double writeScore) : SegmentSummary(segmentId, traffic, latency, iops, traffic_std), read_score(readScore), write_score(writeScore) {}
};

double calculate_segment_score(const int64_t& urgent_traffic, const double& urgent_std, const int64_t& urgent_latency, const int64_t& urgent_iops, const SortType& sortType);

template <typename T>
void AddValues(T& target, uint64_t read_urgent, uint64_t write_urgent, uint64_t read_instant, uint64_t write_instant, uint64_t read_longterm, uint64_t write_longterm);

struct DeviceSummary{
    uint64_t device_id;
    std::vector<uint32_t> segment_index;
    std::vector<SegmentStdStat> segment_traffic_std;
    SumTraffic traffic;
    SumLatency latency;
    SumIops    iops;

    DeviceSummary() = default;
    DeviceSummary(uint64_t device_id) : device_id(device_id), segment_index(), segment_traffic_std(), traffic(), latency(), iops() {}
    void AddResult(uint32_t segment_idx, SegmentStdStat std, uint64_t read_urgent_sum, uint64_t write_urgent_sum, uint64_t read_instant_sum, uint64_t write_instant_sum, uint64_t read_longterm_sum, uint64_t write_longterm_sum, uint64_t read_urgent_latency, uint64_t write_urgent_latency, uint64_t read_instant_latency, uint64_t write_instant_latency, uint64_t read_longterm_latency, uint64_t write_longterm_latency, uint64_t read_urgent_iops, uint64_t write_urgent_iops, uint64_t read_instant_iops, uint64_t write_instant_iops, uint64_t read_longterm_iops, uint64_t write_longterm_iops){
        segment_index.push_back(segment_idx);
        segment_traffic_std.emplace_back(std);
        AddValues(traffic, read_urgent_sum, write_urgent_sum, read_instant_sum, write_instant_sum, read_longterm_sum, write_longterm_sum);
        AddValues(latency, read_urgent_latency, write_urgent_latency, read_instant_latency, write_instant_latency, read_longterm_latency, write_longterm_latency);
        AddValues(iops, read_urgent_iops, write_urgent_iops, read_instant_iops, write_instant_iops, read_longterm_iops, write_longterm_iops);
    }
};
bool operator==(const DeviceSummary& lhs, const DeviceSummary& rhs);

struct BsSumState{
    SumTraffic  mTrafficSum;
    SumLatency  mLatencySum;
    SumIops     mIopsSum;
    BsSumState() : mTrafficSum(), mLatencySum(), mIopsSum() {}

    void AddResult(uint64_t read_urgent_sum, uint64_t write_urgent_sum, uint64_t read_instant_sum, uint64_t write_instant_sum, uint64_t read_longterm_sum, uint64_t write_longterm_sum, uint64_t read_urgent_latency, uint64_t write_urgent_latency, uint64_t read_instant_latency, uint64_t write_instant_latency, uint64_t read_longterm_latency, uint64_t write_longterm_latency, uint64_t read_urgent_iops, uint64_t write_urgent_iops, uint64_t read_instant_iops, uint64_t write_instant_iops, uint64_t read_longterm_iops, uint64_t write_longterm_iops){
        AddValues(mTrafficSum, read_urgent_sum, write_urgent_sum, read_instant_sum, write_instant_sum, read_longterm_sum, write_longterm_sum);
        AddValues(mLatencySum, read_urgent_latency, write_urgent_latency, read_instant_latency, write_instant_latency, read_longterm_latency, write_longterm_latency);
        AddValues(mIopsSum, read_urgent_iops, write_urgent_iops, read_instant_iops, write_instant_iops, read_longterm_iops, write_longterm_iops);
    }
};

struct BsSumScoreState : public BsSumState{
    double BsReadScore;
    double BsWriteScore;
    BsSumScoreState() : BsReadScore(0.0), BsWriteScore(0.0) {}

    void AddScore(int sort_type, double w1, uint64_t read_urgent_sum, uint64_t write_urgent_sum, double read_urgent_std, double write_urgent_std, uint64_t read_urgent_latency, uint64_t write_urgent_latency, uint64_t read_urgent_iops, uint64_t write_urgent_iops){
        SortType sortType = static_cast<SortType>(sort_type);
        if (sortType == SortType::TrafficScore){
            if (read_urgent_latency != 0) {
                BsReadScore += read_urgent_sum * read_urgent_iops / read_urgent_latency;
            }
            if (write_urgent_latency != 0) {
                BsWriteScore += write_urgent_sum * write_urgent_iops / write_urgent_latency;
            }
        }
        else if (sortType == SortType::TrafficStdScore){
            if (read_urgent_latency != 0) {
                BsReadScore += (w1 * read_urgent_sum - (1-w1) * read_urgent_std) * read_urgent_iops / read_urgent_latency;
            }
            if (write_urgent_latency != 0) {
                BsWriteScore += (w1 * write_urgent_sum - (1-w1) * write_urgent_std) * write_urgent_iops / write_urgent_latency;
            }
        }
        else {
            std::cerr << "Invalid sort type: " << sort_type << std::endl;
            exit(EXIT_FAILURE);
        }
    }
};
void calculate_bs_score(std::map<std::string, BsSumScoreState>& bs_score_flow, double w1);

typedef std::map<std::string, std::map<uint64_t, DeviceSummary>> BsDeviceTrafficMap; 
typedef std::map<std::string, std::vector<SegmentSummary>> BsSegTrafficMap;  
typedef std::map<std::string, std::vector<SegmentScoreSummary>> BsSegScoreMap;  
struct BlastRadius{
    double avgblastradius;
    int16_t maxblastradius;
};

struct ReturnSegStat{
    std::map<std::string, BsSumState> bs_flow;
    std::map<std::string, std::vector<SegmentSummary>> sortSegMap;
    BlastRadius blastRadius;
};

struct ReturnDevStat{
    std::map<std::string, BsSumState> bs_flow;
    std::map<std::string, std::vector<DeviceSummary>> sortDevMap;
    BlastRadius blastRadius;
};

struct ReturnRwSegStat{
    std::map<std::string, BsSumState> bs_flow;
    std::map<std::string, std::vector<SegmentSummary>> sortReadSegMap;
    std::map<std::string, std::vector<SegmentSummary>> sortWriteSegMap;
    BlastRadius blastRadius;
};

struct ReturnRwDevStat{
    std::map<std::string, BsSumState> bs_flow;
    std::map<std::string, std::vector<DeviceSummary>> sortReadDevMap;
    std::map<std::string, std::vector<DeviceSummary>> sortWriteDevMap;
    BlastRadius blastRadius;
};

struct ReturnRwSegScoreStat{
    std::map<std::string, BsSumScoreState> bs_score_flow;
    std::map<std::string, std::vector<SegmentScoreSummary>> sortReadSegMap;
    std::map<std::string, std::vector<SegmentScoreSummary>> sortWriteSegMap;
    BlastRadius blastRadius;
};

std::vector<SegmentShmIoStat> read_segment_iostats_mmap(const std::string& path);

std::string bs_ip_transform(uint64_t bsId);

std::map<uint64_t, std::string> bsIdToIp;
std::string bs_ip_transform_cache(uint64_t bsId);

int16_t sortBsSegMap(BsSegTrafficMap& bssegmap, std::string sort_type, int sort_flag, double w_traffic=0.7, double w_read_traffic_ratio=0.3);
int16_t sortBsDevMap(BsDeviceTrafficMap& bsdevicemap, std::map<std::string, std::vector<DeviceSummary>>& sortedBsMap, int& bs_device_num, std::string sort_type, int sort_flag);
int16_t sortBsSegScoreMap(BsSegScoreMap& bssegmap, std::string sort_type);  

extern "C" std::map<std::string, BsSumState> bs_stat();
extern "C" ReturnSegStat merge_bs_segment(int sort_flag=0);
extern "C" ReturnDevStat merge_bs_device(int sort_flag=0);
extern "C" ReturnRwSegStat merge_bs_rw_segment(int r_sort_flag=0, int w_sort_flag=0, double w_traffic=0.7, double w_read_traffic_ratio=0.3);
extern "C" ReturnRwDevStat merge_bs_rw_device(int r_sort_flag=0, int w_sort_flag=0);

#endif