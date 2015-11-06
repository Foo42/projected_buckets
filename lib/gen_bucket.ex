defmodule ProjectedBuckets.GenBucket do
  use GenServer
  require Logger

  def start_link() do
    {:ok, change_streamer} = GenEvent.start_link
    initial_state = %{data: %{}, changes: change_streamer, following: %{}, views: %{}}
    GenServer.start_link __MODULE__, initial_state
  end

  def start_link(mapping_function) do
    {:ok, change_streamer} = GenEvent.start_link
    initial_state = %{data: %{}, following: %{}, changes: change_streamer, mapping_function: mapping_function, views: %{}}
    GenServer.start_link __MODULE__, initial_state
  end

  def follow(bucket, bucket_to_follow), do: GenServer.call(bucket, {:follow, bucket_to_follow})

  def put(bucket, key, value), do: GenServer.call(bucket, {:put, {key, value}})
  def get(bucket, key), do: GenServer.call(bucket, {:get, key})
  def stream_changes(bucket), do: GenServer.call(bucket, {:get_change_stream})
  def install_view(bucket, view_name, mapping_function), do: GenServer.call(bucket, {:install_view, {view_name, mapping_function}})
  def get(bucket, view_name, key), do: GenServer.call(bucket, {:get_using_view, {view_name, key}})

#### Server Implementation ####

  def init(initial_state) do
    {:ok, initial_state}
  end

  def handle_call({:put, {key, value}}, _from, state = %{changes: change_streamer}) do
    commands = run_through_mapping_function(state,key,value) |> Enum.map(&{:put, &1})

    commands |> Enum.each &GenEvent.notify(change_streamer,&1)

    updated_data = commands |> Enum.reduce state.data, &process_command/2
    updated_state = %{state | data: updated_data}
    {:reply, :ok, updated_state}
  end

  def handle_call({:get, key}, _from, state = %{data: data}) do
    {:reply, Map.get(data, key), state}
  end

  def handle_call({:get_using_view, {view_name, key}}, _from, state = %{views: views}) do
    value = case Map.get(views, view_name) do
      nil -> {:error, :unknown_view}
      %{following_process: view_bucket} -> get(view_bucket, key)
    end
    {:reply, value, state}
  end

  def handle_call({:get_change_stream}, _from, state = %{changes: change_streamer, data: data}) do
    change_stream = GenEvent.stream(change_streamer)
    original_data = data |> Enum.map(&{:put, &1})
    stream = Stream.concat(original_data, change_stream)
    {:reply, stream, state}
  end

  def handle_call({:install_view, {view_name, mapping_function}}, from, state) when is_function(mapping_function) do
    {:ok, follower} = start_link(mapping_function)
    handle_call({:install_view, {view_name, follower}}, from, state)
  end

  def handle_call({:install_view, {view_name, follower}}, _from, state = %{views: views}) do
    view = %{following_process: follower}
    updated_views = views |> Map.put(view_name, view)
    follow(follower, self())
    {:reply, {:ok, follower}, %{state | views: updated_views}}
  end

  def handle_call({:follow, target}, _from, state) do
    follower = self()
    spawn_link fn ->
      stream_changes(target)
        |> Stream.each(fn {:put, {key, value}} -> put(follower, key, value) end)
        |> Stream.run
    end

    #Probably, should monitor things we're following?? although the streams should just end?
    {:reply, :ok, %{state | following: Map.put(state.following, target, target)}}
  end

  defp run_through_mapping_function(%{mapping_function: f}, key, value), do: f.({key, value}) |> ensure_list
  defp run_through_mapping_function(_, key, value), do: {key, value} |> ensure_list
  defp ensure_list(input) when is_list(input), do: input
  defp ensure_list(input), do: [input]
  defp process_command({:put, {key,value}}, data), do: Map.put(data, key,value)

end
