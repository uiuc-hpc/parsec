include(ParsecCompilePTG)

parsec_addtest(C rtt "main.c;rtt_wrapper.c;rtt_data.c")
target_ptg_sources(rtt PRIVATE "rtt.jdf")

parsec_addtest(C bw_test)
target_ptg_sources(bw_test PRIVATE "bandwidth.jdf")

parsec_addtest(C bw_overlap_test)
target_ptg_sources(bw_overlap_test PRIVATE "bandwidth_overlap.jdf")

parsec_addtest(C bcast_test)
target_ptg_sources(bcast_test PRIVATE "bcast.jdf")

parsec_addtest(C bcast_bug_test)
target_ptg_sources(bcast_bug_test PRIVATE "bcast_bug.jdf")
