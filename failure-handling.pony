use "collections"
use "format"
use "time"
use "random"

actor Main
  new create(env: Env) =>
    try
      if env.args.size() != 4 then
        env.err.print("Usage: ./chord <num_nodes> <num_requests> <num_failures>")
        error
      end
      
      let num_nodes = env.args(1)?.usize()?
      let num_requests = env.args(2)?.usize()?
      let num_failures = env.args(3)?.usize()?
      
      if num_nodes < 1 then
        env.err.print("The number of nodes must be at least one")
        error
      end

      if num_failures >= num_nodes then
        env.err.print("The number of failures must be less than the number of nodes")
        error
      end
      
      env.out.print("Starting Chord network with " + num_nodes.string() + " nodes, " + 
        num_requests.string() + " requests per node, and " + num_failures.string() + " node failures")
      
      if (num_nodes == 1) or (num_requests < 1) then
        env.out.print("Total average hops for the whole network is 0")
      else
        var temp: USize = num_nodes - 1
        var m: USize = 1  // Start at 1 to account for the +1 in the formula
        while temp > 1 do
          temp = temp >> 1  // Divide by 2
          m = m + 1
        end
        env.out.print("m value" + m.string() + " - " +  num_nodes.string())
        let chord = ChordNetwork(env, num_nodes, num_requests, num_failures, m)
      end
    else
      env.err.print("Error: Invalid arguments")
    end

actor ChordNetwork
  let env: Env
  let _m: USize
  let _num_nodes: USize
  let _num_requests: USize
  let _num_failures: USize
  var _nodes: Map[USize, ChordNode tag]
  var _total_hops: USize = 0
  var _completed_requests: USize = 0
  var _nodes_ready: USize = 0
  var _current_node: USize = 1
  var _stabilization_count: USize = 0
  let _rand: Random
  var _failed_nodes: Set[USize]
  var _stabilization_active: Bool = false
  let _timers: Timers
  
  new create(env': Env, num_nodes: USize, num_requests: USize, num_failures: USize, m: USize) =>
    env = env'
    _m = m
    _num_nodes = num_nodes
    _num_requests = num_requests
    _num_failures = num_failures
    _nodes = Map[USize, ChordNode tag]
    _rand = Rand(Time.nanos())
    _failed_nodes = Set[USize]
    
    // Setup periodic stabilization timer
    _timers = Timers
    let timer = Timer(StabilizationNotify(this), 0, 1_000_000_000) // Run every 1 second
    _timers(consume timer)
    
    env.out.print("Creating first node...")
    create_first_node()
    
  be create_first_node() =>
    let first_id: USize = 0
    let first_node = ChordNode(this, first_id, _m)
    _nodes(first_id) = first_node
    
    // Initialize node 0 with some key-value pairs
    let initial_data = recover val
      let m = Map[USize, String]
      for i in Range(0, 20) do
        m(i) = "value" + i.string()
      end
      consume m
    end
    
    first_node.initialize_storage(initial_data)
    env.out.print("Created first node with ID: " + first_id.string())
    _network_stabilized()

  be _network_stabilized() =>
    if _current_node < _num_nodes then
      create_next_node()
    else
      env.out.print("All nodes created. Starting requests...")
      start_requests()
      simulate_failures() // Simulate failures after starting requests
      _stabilization_active = true // Start periodic stabilization
    end
    
  be create_next_node() =>
    let node_id = _current_node * ((1 << _m) / _num_nodes)
    let new_node = ChordNode(this, node_id, _m)
    _nodes(node_id) = new_node
    env.out.print("Created node " + _current_node.string() + " with ID: " + node_id.string())
    
    try
      let existing_node = _nodes(0)?
      existing_node.join(node_id, create_val_map())
    end
    
    _current_node = _current_node + 1
    initiate_stabilization()
    
  be route_message(from_id: USize, to_id: USize, msg: String) =>
    try
      if not _failed_nodes.contains(to_id) then
        _nodes(to_id)?.receive_message(from_id, msg)
      end
    end
    
  fun ref create_val_map(): Map[USize, ChordNode tag] val =>
    let temp = recover trn Map[USize, ChordNode tag] end
    for (k, v) in _nodes.pairs() do
      temp(k) = v
    end
    consume temp

  fun ref create_failed_nodes_val(): Set[USize] val =>
    let temp = recover trn Set[USize] end
    for id in _failed_nodes.values() do
      temp.set(id)
    end
    consume temp
    
  be initiate_stabilization() =>
    _stabilization_count = 0
    env.out.print("Initiating stabilization round...")
    let nodes_val = create_val_map()
    let failed_nodes_val = create_failed_nodes_val()
    for (id, node) in _nodes.pairs() do
      if not _failed_nodes.contains(id) then
        node.stabilize(nodes_val, failed_nodes_val)
      end
    end
    
  be stabilization_complete() =>
    _stabilization_count = _stabilization_count + 1
    let active_nodes = _nodes.size() - _failed_nodes.size()
    if _stabilization_count == active_nodes then
      env.out.print("Stabilization round complete")
      _network_stabilized()
    end

  be simulate_failures() =>
    env.out.print("\nSimulating node failures...")
    let node_ids = Array[USize]
    for id in _nodes.keys() do
      node_ids.push(id)
    end
    
    var failures_left = _num_failures
    while failures_left > 0 do
      let index = _rand.int[USize](node_ids.size())
      try
        let actual_id = node_ids(index)?
        if not _failed_nodes.contains(actual_id) then
          _failed_nodes.set(actual_id)
          env.out.print("Node " + actual_id.string() + " has failed")
          failures_left = failures_left - 1
        end
      end
    end

  be periodic_stabilize() =>
    if (_nodes.size() > 0) and _stabilization_active then
      env.out.print("\nRunning periodic stabilization...")
      let nodes_val = create_val_map()
      let failed_nodes_val = create_failed_nodes_val()
      for (id, node) in _nodes.pairs() do
        if not _failed_nodes.contains(id) then
          node.stabilize(nodes_val, failed_nodes_val)
        end
      end
    end

    
  be start_requests() =>
    let nodes_val = create_val_map()
    for (id, node) in _nodes.pairs() do
      if not _failed_nodes.contains(id) then
        node.start_requests(_num_requests, nodes_val)
      end
    end
    
  be record_hop() =>
    _total_hops = _total_hops + 1

  be request_complete() =>
    _completed_requests = _completed_requests + 1
    let total_expected = (_num_nodes - _num_failures) * _num_requests
    //env.out.print("Request completed: " + _completed_requests.string() + "/" + total_expected.string())
    
    if _completed_requests == total_expected then
      _stabilization_active = false
      calculate_avg_hops()
    end

  be calculate_avg_hops() =>
    env.out.print("All requests completed!")
    env.out.print("Total hops: " + _total_hops.string())
    let total_completed = _completed_requests
    env.out.print("Total successful requests: " + total_completed.string())
    let avg_hops = _total_hops.f64() / total_completed.f64()
    env.out.print("Average hops per request: " + avg_hops.string())
    env.out.print("-------- FINISHED ---------")
    _timers.dispose() 
    env.exitcode(0)

  be print_message(msg: String) =>
    env.out.print(msg)

actor ChordNode
  let _network: ChordNetwork tag
  let _id: USize
  let _m: USize
  let _finger_table: Array[USize]
  let _rand: Random
  var _predecessor: (USize | None) = None
  var _successor: USize
  let _storage: Map[USize, String]
  // Add successor list for fault tolerance
  let _successor_list: Array[USize]
  let _successor_list_size: USize = 4 // Maintain multiple successors for redundancy

  new create(network: ChordNetwork tag, id: USize, m: USize) =>
    _network = network
    _id = id
    _m = m
    _finger_table = Array[USize].init(id, m)
    _successor = id
    _rand = Rand(Time.nanos())
    _storage = Map[USize, String]
    _successor_list = Array[USize].init(id, _successor_list_size)

  be stabilize(nodes: Map[USize, ChordNode tag] val, failed_nodes: Set[USize] val) =>
    // Update successor list first
    update_successor_list(nodes, failed_nodes)
    
    // Use first valid successor from list
    try
      for i in Range(0, _successor_list_size) do
        let potential_successor = _successor_list(i)?
        if not failed_nodes.contains(potential_successor) then
          _successor = potential_successor
          break
        end
      end
    end
    
    _network.route_message(_id, _successor, "get_predecessor")
    fix_fingers(nodes, failed_nodes)
    _network.route_message(_id, _successor, "notify:" + _id.string())
    _network.stabilization_complete()

  fun ref update_successor_list(nodes: Map[USize, ChordNode tag] val, failed_nodes: Set[USize] val) =>
    try
      var current = _id
      var count: USize = 0
      while count < _successor_list_size do
        current = find_next_valid_successor(current, nodes, failed_nodes)
        _successor_list.update(count, current)?
        count = count + 1
      end
    end

  fun find_next_valid_successor(start: USize, nodes: Map[USize, ChordNode tag] val, 
    failed_nodes: Set[USize] val): USize =>
    var next = (start + 1) % (1 << _m)
    while failed_nodes.contains(next) do
      next = (next + 1) % (1 << _m)
    end
    next

  fun ref fix_fingers(nodes: Map[USize, ChordNode tag] val, failed_nodes: Set[USize] val) =>
    try
      for i in Range(0, _m) do
        let target = (_id + (1 << i)) % (1 << _m)
        var successor = find_best_successor(target, nodes, failed_nodes)
        _finger_table.update(i, successor)?
      end
    end

  fun find_best_successor(target: USize, nodes: Map[USize, ChordNode tag] val, 
    failed_nodes: Set[USize] val): USize =>
    var best_successor = _id
    var min_distance = USize.max_value()
    
    for n in nodes.keys() do
      if not failed_nodes.contains(n) then
        let distance = calculate_distance(n, target)
        if (distance < min_distance) then
          best_successor = n
          min_distance = distance
        end
      end
    end
    
    best_successor

  fun calculate_distance(from: USize, to: USize): USize =>
    if to >= from then
      to - from
    else
      (1 << _m) - (from + to)
    end

  fun closest_preceding_finger(id: USize): USize =>
    try
      var i: USize = _m
      while i > 0 do
        i = i - 1
        let finger = _finger_table(i)?
        if in_range(finger, _id, id) then
          return finger
        end
      end
    end
    
    // If no valid finger found, use successor list
    try
      for i in Range(0, _successor_list_size) do
        let succ = _successor_list(i)?
        if in_range(succ, _id, id) then
          return succ
        end
      end
    end
    
    _id

  be lookup(key: USize, nodes: Map[USize, ChordNode tag] val, hops: USize) =>
    _network.record_hop()
    
    if key == _id then
      _network.request_complete()
      return
    end
    
    try
      let closest_preceding = closest_preceding_finger(key)
      if (closest_preceding == _id) then
        // Try successor list if finger table fails
        for i in Range(0, _successor_list_size) do
          let succ = _successor_list(i)?
          if nodes.contains(succ) then
            nodes(succ)?.lookup(key, nodes, hops + 1)
            return
          end
        end
      end
      
      if nodes.contains(closest_preceding) then
        nodes(closest_preceding)?.lookup(key, nodes, hops + 1)
        return
      end
    end
    
    _network.request_complete()

  fun in_range(key: USize, start: USize, endd: USize): Bool =>
    if start < endd then
      (key > start) and (key <= endd)
    else
      (key > start) or (key <= endd)
    end

  be initialize_storage(initial_data: Map[USize, String] val) =>
    for (k, v) in initial_data.pairs() do
      _storage(k) = v
    end
    _network.print_message("Node 0 initialized with " + _storage.size().string() + " key-value pairs")

  be join(node_id: USize, nodes: Map[USize, ChordNode tag] val) =>
    try
      let n0 = closest_preceding_node(node_id)
      if n0 != _id then
        _network.route_message(_id, n0, "find_successor:" + node_id.string())
      else
        let successor_id = find_successor(node_id)?
        _network.route_message(_id, node_id, "set_successor:" + successor_id.string())
        transfer_keys(node_id)
      end
    end

  fun closest_preceding_node(id: USize): USize =>
    try
      var i: USize = _m
      while i > 0 do
        i = i - 1
        let finger = _finger_table(i)?
        if in_range(finger, _id, id) then
          return finger
        end
      end
    end
    _id

  be transfer_keys(new_node_id: USize) =>
    try
      let keys_to_transfer = Map[USize, String]
      for (k, v) in _storage.pairs() do
        if in_range(k, _predecessor as USize, new_node_id) then
          keys_to_transfer(k) = v
        end
      end
      
      for (k, v) in keys_to_transfer.pairs() do
        _storage.remove(k)?
        _network.route_message(_id, new_node_id, "store_key:" + k.string() + ":" + v)
      end
      
      _network.print_message("Node " + _id.string() + " transferred " + 
        keys_to_transfer.size().string() + " keys to node " + new_node_id.string())
    end


  fun find_successor(id: USize): USize ? =>
    if in_range(id, _id, _successor) then
      _successor
    else
      let n0 = closest_preceding_node(id)
      if n0 != _id then
        _network.route_message(_id, n0, "find_successor:" + id.string())
        error
      end
      _successor
    end


  be receive_message(from_id: USize, msg: String) =>
    try
      if msg.substring(0, 13) == "set_successor:" then
        _successor = msg.substring(13).usize()?
        _predecessor = None
      elseif msg == "get_predecessor" then
        _network.route_message(_id, from_id, "predecessor:" + 
          match _predecessor
          | let p: USize => p.string()
          | None => "none"
          end)
      elseif msg.substring(0, 11) == "predecessor:" then
        let pred_str = msg.substring(11)
        if pred_str != "none" then
          let x = pred_str.usize()?
          if in_range(x, _id, _successor) then
            _successor = x
          end
        end
        _network.route_message(_id, _successor, "notify:" + _id.string())
      elseif msg.substring(0, 7) == "notify:" then
        let potential_pred = msg.substring(7).usize()?
        match _predecessor
        | None => _predecessor = potential_pred
        | let p: USize =>
          if in_range(potential_pred, p, _id) then
            _predecessor = potential_pred
          end
        end
      elseif msg.substring(0, 14) == "find_successor:" then
        let search_id = msg.substring(14).usize()?
        try
          let successor_id = find_successor(search_id)?
          _network.route_message(_id, from_id, "successor_found:" + successor_id.string())
        end
      elseif msg.substring(0, 16) == "successor_found:" then
        let found_successor = msg.substring(16).usize()?
        _network.route_message(_id, found_successor, "set_successor:" + found_successor.string())
      elseif msg.substring(0, 10) == "store_key:" then
        let parts = msg.substring(10).split(":")
        try
          let key = parts(0)?.usize()?
          let value = parts(1)?
          _storage(key) = value
          _network.print_message("Node " + _id.string() + " stored key " + key.string() +  " with value " + value)
        end
      elseif msg.substring(0, 8) == "get_key:" then
        let key = msg.substring(8).usize()?
        try
          let value = _storage(key)?
          _network.route_message(_id, from_id, "key_value:" + key.string() + ":" + value)
        else
          _network.route_message(_id, from_id, "key_not_found:" + key.string())
        end
     end
  end
      
  be start_requests(num_requests: USize, nodes: Map[USize, ChordNode tag] val) =>
    for i in Range(0, num_requests) do
      let target = _rand.int[USize](1 << _m)
      lookup(target, nodes, 0)
    end
    

class StabilizationNotify is TimerNotify
  let _network: ChordNetwork tag

  new iso create(network: ChordNetwork tag) =>
    _network = network

  fun ref apply(timer: Timer, count: U64): Bool =>
    _network.periodic_stabilize()
    true

  fun ref cancel(timer: Timer) =>
    None