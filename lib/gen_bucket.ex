defmodule ProjectedBuckets.GenBucket do
  use GenServer
  require Logger

  def start_link() do
    {:ok, change_streamer} = GenEvent.start_link
    initial_state = %{views: %{__primary: %{data: %{}, changes: change_streamer}}}
    GenServer.start_link __MODULE__, initial_state
  end

  def put(bucket, key, value) do
    update_view(bucket, :__primary, {:put, {key, value}})
  end
  def get(bucket, key), do: GenServer.call(bucket, {:get, key})
  def stream_changes(bucket), do: GenServer.call(bucket, {:get_change_stream})
  def install_view(bucket, view_name, mapping_function), do: GenServer.call(bucket, {:install_view, {view_name, mapping_function}})
  def get(bucket, view_name, key), do: GenServer.call(bucket, {:get_using_view, {view_name, key}})

#### Server Implementation ####

  def init(initial_state) do
    {:ok, initial_state}
  end

  def handle_call(command = {:put, {view_name, key, value}}, _from, state = %{views: views}) do
    updated_views = case Map.get(views, view_name) do
      nil -> views
      view = %{changes: change_streamer} ->
        updated_views = views |> Map.put(view_name, %{view | data: Map.put(view.data, key, value) })
        command = {:put, {key, value}}
        change_streamer |> GenEvent.notify(command)
        updated_views
      view ->
        views |> Map.put(view_name, %{view | data: Map.put(view.data, key, value) })
    end
    updated_state = %{state | views: updated_views}
    {:reply, :ok, updated_state}
  end

  def handle_call({:get, key}, _from, state = %{views: %{__primary: %{data: data}}}) do
    {:reply, Map.get(data, key), state}
  end

  def handle_call({:get_using_view, {view_name, key}}, _from, state = %{views: views}) do
    value = case Map.get(views, view_name) do
      nil -> {:error, :unknown_view}
      view -> Map.get(view.data, key)
    end
    {:reply, value, state}
  end

  def handle_call({:get_change_stream}, _from, state = %{views: views}) do
    view_name = :__primary
    case Map.get(views, view_name) do
      nil -> :error
      view = %{data: data, changes: change_streamer} ->
        change_stream = GenEvent.stream(change_streamer)
        original_data = data |> Enum.map(&{:put, &1})
        stream = Stream.concat(original_data, change_stream)
        {:reply, stream, state}
    end
  end

  def handle_call({:install_view, {view_name, mapping_function}}, _from, state = %{views: views}) do
    view = %{mapping_function: mapping_function, data: %{}}
    updated_views = views |> Map.put(view_name, view)
    begin_view_mapping(self(), view |> Map.put(:view_name, view_name))
    {:reply, :ok, %{state | views: updated_views}}
  end

  defp begin_view_mapping(bucket, %{view_name: view_name, mapping_function: mapping_function}) do
    spawn_link fn ->
      stream_changes(bucket)
        |> Stream.map(fn {:put, key_value} -> {:put, mapping_function.(key_value)} end)
        |> Stream.each(&update_view(bucket, view_name, &1))
        |> Stream.run
    end
  end

  defp update_view(bucket, view_name, {:put, {key, value}}), do: GenServer.call(bucket, {:put, {view_name, key, value}})
end
