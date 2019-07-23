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
  ccp_1_b.value = value;
  version_store[cid_function_pair].second["b"]["b"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_1_b);
  // func 1 key c
  CrossCausalPayload<SetLattice<string>> ccp_1_c;
  ccp_1_c.vector_clock.insert("base", 1);
  ccp_1_c.value = value;
  version_store[cid_function_pair].second["c"]["c"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_1_c);
  
  // func 2
  cid_function_pair = std::make_pair("test_cid", "strmnp2");
  version_store[cid_function_pair].first = false;
  // func 2 key a
  CrossCausalPayload<SetLattice<string>> ccp_2_a;
  ccp_2_a.vector_clock.insert("base", 2);
  ccp_2_a.value = value;
  version_store[cid_function_pair].second["a"]["a"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_2_a);
  // func 2 key b
  CrossCausalPayload<SetLattice<string>> ccp_2_b;
  ccp_2_b.vector_clock.insert("base", 1);
  ccp_2_b.value = value;
  version_store[cid_function_pair].second["b"]["b"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_2_b);
  // func 2 key c
  CrossCausalPayload<SetLattice<string>> ccp_2_c;
  ccp_2_c.vector_clock.insert("base", 1);
  ccp_2_c.value = value;
  version_store[cid_function_pair].second["c"]["c"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_2_c);

  // func 3
  cid_function_pair = std::make_pair("test_cid", "strmnp3");
  version_store[cid_function_pair].first = false;
  // func 3 key d
  CrossCausalPayload<SetLattice<string>> ccp_3_d;
  ccp_3_d.vector_clock.insert("base", 1);
  ccp_3_d.value = value;
  version_store[cid_function_pair].second["d"]["d"] = std::make_shared<CrossCausalLattice<SetLattice<string>>>(ccp_3_d);
}