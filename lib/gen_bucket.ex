defmodule ProjectedBuckets.GenBucket do
  use GenServer
  require Logger

  def start_link() do
    {:ok, change_streamer} = GenEvent.start_link
    initial_state = %{views: %{}, primary_data: %{}, change_streamer: change_streamer}
    GenServer.start_link __MODULE__, initial_state
  end

  def put(bucket, key, value), do: GenServer.call(bucket, {:put, {key, value}})
  def get(bucket, key), do: GenServer.call(bucket, {:get, key})
  def stream_changes(bucket), do: GenServer.call(bucket, {:get_change_stream})
  def install_view(bucket, view_name, mapping_function), do: GenServer.call(bucket, {:install_view, {view_name, mapping_function}})
  def get(bucket, view_name, key), do: GenServer.call(bucket, {:get_using_view, {view_name, key}})

#### Server Implementation ####

  def init(initial_state) do
    {:ok, initial_state}
  end

  def handle_call(command = {:put, {key, value}}, _from, state = %{primary_data: data, change_streamer: change_streamer}) do
    updated_state = %{state | primary_data: data |> Map.put(key, value)}
    change_streamer |> GenEvent.notify(command)
    {:reply, :ok, updated_state}
  end

  def handle_call(command = {:put_in_view, {view_name, key, value}}, _from, state = %{views: views, change_streamer: change_streamer}) do
    updated_views = case Map.get(views, view_name) do
      nil -> views
      view -> Map.put(views, view_name, Map.put(view, key, value))
    end
    updated_state = %{state | views: updated_views}
    {:reply, :ok, updated_state}
  end

  def handle_call({:get, key}, _from, state = %{primary_data: data}) do
    {:reply, Map.get(data, key), state}
  end

  def handle_call({:get_using_view, {view_name, key}}, _from, state = %{views: views}) do
    value = case Map.get(views, view_name) do
      nil -> {:error, :unknown_view}
      view -> Map.get(view, key)
    end
    {:reply, value, state}
  end

  def handle_call({:get_change_stream}, _from, state = %{change_streamer: change_streamer, primary_data: data}) do
    change_stream = GenEvent.stream(change_streamer)
    original_data = data |> Enum.map(&{:put, &1})
    stream = Stream.concat(original_data, change_stream)
    {:reply, stream, state}
  end

  def handle_call({:install_view, {view_name, mapping_function}}, _from, state = %{views: views, change_streamer: change_streamer}) do
    view = %{mapping_function: mapping_function, data: %{}}
    updated_views = views |> Map.put(view_name, view)
    begin_view_mapping(self(), state, view |> Map.put(:view_name, view_name))
    {:reply, :ok, %{state | views: updated_views}}
  end

  defp begin_view_mapping(bucket, %{change_streamer: change_streamer}, %{view_name: view_name, mapping_function: mapping_function}) do
    spawn_link fn ->
      stream_changes(bucket)
        |> Stream.each(&IO.puts("about to map #{inspect &1} for view #{view_name}"))
        |> Stream.map(fn {:put, key_value} -> {:put, mapping_function.(key_value)} end)
        |> Stream.each(&IO.puts("mapped to #{inspect &1} for view #{view_name}"))
        |> Stream.each(&update_view(bucket, view_name, &1))
        |> Stream.each(&IO.puts("updated view #{view_name} with #{inspect &1}"))
        |> Stream.run
    end
  end

  defp update_view(bucket, view_name, {:put, {key, value}}), do: GenServer.call(bucket, {:put_in_view, {view_name, key, value}})
end
