defmodule ProjectedBuckets.GenBucket do
  def start_link() do
    Agent.start_link(fn -> %{primary_data: %{}} end)
  end

  def put(bucket, key, value) do
    Agent.update(bucket, fn all_state -> %{all_state | primary_data: all_state.primary_data |> Map.put(key, value)}  end)
  end

  def get(bucket, key) do
    case Agent.get(bucket, fn all_state -> all_state.primary_data |> Map.fetch(key) end) do
      {:ok, value} -> value
      other -> other
    end
  end
end
