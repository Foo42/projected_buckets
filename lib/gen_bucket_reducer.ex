defmodule ProjectedBuckets.GenBucketReducer do
    use GenServer
    require Logger

    def start_link(initial_value, reducer_fn), do: GenServer.start(__MODULE__, %{initial_value: initial_value, reducer_fn: reducer_fn, reduced_value: :unavailable, bucket_contents: %{}})
    def follow(me, target_bucket), do: GenServer.call(me, {:follow, target_bucket})
    defp update_internal_bucket(who, command), do: GenServer.call(who, {:process_update, command})
    def value(who), do: GenServer.call(who, {:get_value})

    #####################

    def handle_call({:follow, target_bucket}, _from, state) do
        follower = self()
        Logger.info "#{inspect follower} starting to follow #{inspect target_bucket}"
        spawn_link fn ->
          ProjectedBuckets.GenBucket.stream_changes(target_bucket)
            |> Stream.each(&update_internal_bucket(follower,&1))
            |> Stream.run
        end
        {:reply, :ok, state}
    end

    def handle_call({:process_update, {:put, {k, v}}}, _from, state = %{bucket_contents: data}) do
      {:reply, :ok, %{ state | reduced_value: :stale, bucket_contents: data |> Map.put(k,v)}}
    end

    def handle_call({:get_value}, _from, state = %{reduced_value: :stale}) do
      %{reducer_fn: reducer_fn, bucket_contents: bucket_contents, initial_value: initial_value} = state
      reduced_value = bucket_contents |> Enum.reduce initial_value, reducer_fn
      {:reply, reduced_value, %{state | reduced_value: reduced_value}}
    end

    def handle_call({:get_value}, _from, state = %{reduced_value: reduced_value}) do
      {:reply, reduced_value, state}
    end
end
