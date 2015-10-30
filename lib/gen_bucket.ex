defmodule ProjectedBuckets.GenBucket do
  def start_link() do
    {:ok, change_streamer} = GenEvent.start_link
    Agent.start_link(fn -> %{views: %{}, primary_data: %{}, change_streamer: change_streamer} end)
  end

  def put(bucket, key, value) do
    Agent.update(bucket, fn all_state ->
      updated = %{all_state | primary_data: all_state.primary_data |> Map.put(key, value)}
      all_state.change_streamer |> GenEvent.notify {:put, {key, value}}
      updated
    end)
  end

  def get(bucket, key) do
    case Agent.get(bucket, fn all_state -> all_state.primary_data |> Map.fetch(key) end) do
      {:ok, value} -> value
      other -> other
    end
  end

  def get(bucket, view_name, key) do
    Agent.get(bucket, fn all_state ->
      case Map.fetch(all_state.views, view_name) do
        {:ok, view} -> value_or_error(Map.fetch(view, key))
        other -> {:error, :unknown_view}
      end
    end)
  end

  def value_or_error(thing) do
    case thing do
      {:ok, value} -> value
      other -> other
    end
  end

  def stream_changes(bucket) do
    IO.puts inspect bucket
    Agent.get(bucket, fn all_state -> all_state.change_streamer end) |> GenEvent.stream
  end

  def install_view(bucket, view_name, mapping_function) do
    Agent.update(bucket, fn all_state ->
      mapping_proc = spawn_link fn ->
        IO.puts "mapping proc started for view #{view_name}"
        all_state.change_streamer
          |> GenEvent.stream
          |> Stream.each(&IO.puts("about to map #{inspect &1} for view #{view_name}"))
          |> Stream.map(fn {:put, key_value} -> {:put, mapping_function.(key_value)} end)
          |> Stream.each(&IO.puts("mapped to #{inspect &1} for view #{view_name}"))
          |> Stream.each(&update_view(bucket, view_name, &1))
          |> Stream.each(&IO.puts("updated view #{view_name} with #{inspect &1}"))
          |> Stream.run
      end
      view = %{mapping_function: mapping_function, data: %{}, mapping_proc: mapping_proc}
      updated_views = all_state.views |> Map.put view_name, view
      %{all_state | views: updated_views}
    end)
  end

  defp update_view(bucket, view_name, update = {:put, {key,value}}) do
    IO.puts "in update_view for #{view_name}, with updated = #{inspect update}"
    view_update = fn view_data -> Map.put(view_data, key, value) end
    all_views_update = fn all_views -> all_views |> Map.put(view_name, view_update.(Map.get(all_views, view_name ))) end
    bucket
      |> Agent.update(fn all_state -> %{all_state | views: all_views_update.(all_state.views)} end)
  end
end
