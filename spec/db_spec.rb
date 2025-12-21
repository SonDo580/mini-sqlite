describe "database" do
  before do
    `rm -rf test.db`
  end

  def run_script(commands)
    raw_output = nil

    # Spawn a child OS process running "./db.o" 
    # "r+" give us a bidirectional pipe
    # - write to process's stdin
    # - read from process's stdout
    IO.popen("./db.o test.db", "r+") do |pipe|
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
    id = 1
    username = "user1"
    email = "user1@x.com"
    result = run_script([
      "insert #{id} #{username} #{email}",
      "select",
      ".exit"
    ])
    expect(result).to match_array([
      "db > Executed.",
      "db > (#{id}, #{username}, #{email})",
      "Executed.",
      "db > ",
    ])
  end

  it 'keep data after closing connection' do
    id = 1
    username = "user1"
    email = "user1@x.com"

    result1 = run_script([
      "insert #{id} #{username} #{email}",
      ".exit"
    ])
    expect(result1).to match_array([
      "db > Executed.",
      "db > ",
    ])

    result2 = run_script([
      "select",
      ".exit"
    ])
    expect(result2).to match_array([
      "db > (#{id}, #{username}, #{email})",
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
    id = 1
    username = "a"*32
    email = "a"*255
    script = [
      "insert #{id} #{username} #{email}",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > Executed.",
      "db > (#{id}, #{username}, #{email})",
      "Executed.",
      "db > ",
    ])
  end

  it 'print error if strings are too long' do
    id = 1
    username = "a"*33
    email = "a"*256
    script = [
      "insert #{id} #{username} #{email}",
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

  it 'print constants' do
    script = [
      ".constants",
      ".exit",
    ]
    result = run_script(script)

    expect(result).to match_array([
      "db > Constants:",
      "ROW_SIZE: 293",
      "COMMON_NODE_HEADER_SIZE: 6",
      "LEAF_NODE_HEADER_SIZE: 10",
      "LEAF_NODE_CELL_SIZE: 297",
      "LEAF_NODE_SPACE_FOR_CELLS: 4086",
      "LEAF_NODE_MAX_CELLS: 13",
      "db > ",
    ])
  end

  it 'print structure of a 1-node btree' do
    script = [3, 1, 2].map do |i|
      "insert #{i} user#{i} user#{i}@x.com"
    end
    script << '.btree'
    script << '.exit'
    result = run_script(script)

    expect(result).to match_array([
      "db > Executed.",
      "db > Executed.",
      "db > Executed.",
      "db > Tree:",
      "leaf (size 3)",
      " - 0 : 3",
      " - 1 : 1",
      " - 2 : 2",
      "db > "
    ])
  end
end