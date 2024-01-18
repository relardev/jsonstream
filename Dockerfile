# Step 1: Use the official Elixir image as a base
FROM elixir:latest AS build

# Step 2: Set environment to production
ENV MIX_ENV=prod

# Step 3: Install Hex + Rebar
RUN mix local.hex --force && \
    mix local.rebar --force

# Set the working directory inside the container
WORKDIR /app

# Copy all dependencies files
COPY mix.exs mix.lock ./

# Fetch the application dependencies and build the application
RUN mix deps.get
RUN mix deps.compile

# Step 4: Compile the application
COPY . .
RUN mix compile

# Step 5: Build the release
RUN mix escript.build

# Step 6: Prepare release image
FROM erlang:latest AS runtime

WORKDIR /app

# Copy the release build from the previous stage
COPY --from=build /app/jk_elixir ./js

ENV PATH="/app:${PATH}"

ENV LC_ALL=C.UTF-8
# Step 7: Run the application
CMD ["js"]
