void warmup(VersionStoreType& version_store) {
  SetLattice<string> value;
  value.insert("00000");
  // func 1
  ClientIdFunctionPair cid_function_pair = std::make_pair("test_cid", "strmnp1");
  version_store[cid_function_pair].first = false;
  // func 1 key a
  CrossCausalPayload<SetLattice<string>> ccp_1_a;
  ccp_1_a.vector_clock.insert("base", 1);
  ccp_1_a.value = value;
  version_store[cid_function_pair].second["a"]["a"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_1_a);
  // func 1 key b
  CrossCausalPayload<SetLattice<string>> ccp_1_b;
  ccp_1_b.vector_clock.insert("base", 1);
  ccp_1_b.dependency.insert("d", VectorClock({{"base", 1}}));
  ccp_1_b.value = value;
  version_store[cid_function_pair].second["b"]["b"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_1_b);
  CrossCausalPayload<SetLattice<string>> ccp_1_b_d;
  ccp_1_b_d.vector_clock.insert("base", 1);
  ccp_1_b_d.value = value;
  version_store[cid_function_pair].second["b"]["d"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_1_b_d);
  // func 1 key c
  CrossCausalPayload<SetLattice<string>> ccp_1_c;
  ccp_1_c.vector_clock.insert("base", 1);
  ccp_1_c.value = value;
  version_store[cid_function_pair].second["c"]["c"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_1_c);
  
  // func 2
  cid_function_pair = std::make_pair("test_cid", "strmnp2");
  version_store[cid_function_pair].first = false;
  // func 2 key d
  CrossCausalPayload<SetLattice<string>> ccp_2_d;
  ccp_2_d.vector_clock.insert("base", 2);
  ccp_2_d.dependency.insert("a", VectorClock({{"base", 2}}));
  ccp_2_d.value = value;
  version_store[cid_function_pair].second["d"]["d"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_2_d);
  CrossCausalPayload<SetLattice<string>> ccp_2_d_a;
  ccp_2_d_a.vector_clock.insert("base", 2);
  ccp_2_d_a.value = value;
  version_store[cid_function_pair].second["d"]["a"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_2_d_a);
  // func 2 key e
  CrossCausalPayload<SetLattice<string>> ccp_2_e;
  ccp_2_e.vector_clock.insert("base", 2);
  ccp_2_e.dependency.insert("d", VectorClock({{"base", 2}}));
  ccp_2_e.value = value;
  version_store[cid_function_pair].second["e"]["e"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_2_e);
  CrossCausalPayload<SetLattice<string>> ccp_2_e_d;
  ccp_2_e_d.vector_clock.insert("base", 2);
  ccp_2_e_d.value = value;
  version_store[cid_function_pair].second["e"]["d"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_2_e_d);

  // func 3
  cid_function_pair = std::make_pair("test_cid", "strmnp3");
  version_store[cid_function_pair].first = false;
  // func 3 key f
  CrossCausalPayload<SetLattice<string>> ccp_3_f;
  ccp_3_f.vector_clock.insert("base", 1);
  ccp_3_f.value = value;
  version_store[cid_function_pair].second["f"]["f"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_3_f);
  // func 3 key g
  CrossCausalPayload<SetLattice<string>> ccp_3_g;
  ccp_3_g.vector_clock.insert("base", 1);
  ccp_3_g.value = value;
  version_store[cid_function_pair].second["g"]["g"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_3_g);
}