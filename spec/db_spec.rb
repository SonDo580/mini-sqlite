describe "database" do
  def run_script(commands)
    raw_output = nil

    # Spawn a child OS process running "./db.o" 
    # "r+" give us a bidirectional pipe
    # - write to process's stdin
    # - read from process's stdout
    IO.popen("./db.o", "r+") do |pipe|
      # Send each command to DB process via stdin
      commands.each do |command|
        pipe.puts command 
      end

      # Close stdin
      pipe.close_write

      # Read entire stdout of DB process
      raw_output = pipe.gets(nil)
    end

    # Return output as array of lines
    raw_output.split("\n")
  end

  it 'insert and retrieve a row' do
    result = run_script([
      "insert 1 user1 user1@x.com",
      "select",
      ".exit"
    ])
    expect(result).to match_array([
      "db > Executed.",
      "db > (1, user1, user1@x.com)",
      "Executed.",
      "db > ",
    ])
  end

  it 'print error when table is full' do
    # row_size = sizeof(u_int_32) + sizeof(char*) = 4 + 32 + 255 = 291 (bytes)
    # page_size = 4096 (bytes)
    # table_max_pages = 100
    # -> rows_per_page = floor(4906 / 291) = 14
    # -> table_max_rows = 14 * 100 = 1400
    # -> try inserting 1401 rows
    script = (1..1401).map do |i|
      "insert #{i} user#{i} user#{i}@x.com"
    end
    script << ".exit"

    result = run_script(script)
    expect(result[-2]).to eq("db > Error: Table full.")
  end

  it 'allow inserting strings with maximum length' do
    username = "a"*32
    email = "a"*255
    script = [
      "insert 1 #{username} #{email}",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > Executed.",
      "db > (1, #{username}, #{email})",
      "Executed.",
      "db > ",
    ])
  end

  it 'print error if strings are too long' do
    username = "a"*33
    email = "a"*256
    script = [
      "insert 1 #{username} #{email}",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > String is too long.",
      "db > Executed.",
      "db > ",
    ])
  end

  it 'print error if id is negative' do
    script = [
      "insert -1 cstack foo@bar.com",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > ID must be positive.",
      "db > Executed.",
      "db > ",
    ])
  end 
end